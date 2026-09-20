use std::cell::RefCell;
use std::future::{self, Future};
use std::mem;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use std::collections::{HashMap, HashSet};

use wasi::io::poll::Pollable as WasiPollable;

use tracing::{instrument, trace};

use crate::poller::{EventKey, Poller};

pub(crate) fn task_waker(state: Rc<RefCell<bool>>) -> Waker {
    const VTABLE: RawWakerVTable = {
        unsafe fn clone(ptr: *const ()) -> RawWaker { unsafe {
            let ptr = ptr as *const RefCell<bool>;
            Rc::increment_strong_count(ptr);

            let state = Rc::from_raw(ptr);

            let state = state.clone();

            RawWaker::new(Rc::into_raw(state) as _, &VTABLE)
        }}

        unsafe fn wake(ptr: *const ()) { unsafe {
            let state = Rc::from_raw(ptr as *const RefCell<bool>);
            if let Ok(state) = state.try_borrow_mut().as_mut() {
                **state = true;
            };
        }}

        unsafe fn wake_by_ref(ptr: *const ()) { unsafe {
            let ptr = ptr as *const RefCell<bool>;
            Rc::increment_strong_count(ptr);

            let state = Rc::from_raw(ptr);
            if let Ok(state) = state.try_borrow_mut().as_mut() {
                **state = true;
            };
        }}

        unsafe fn drop(ptr: *const ()) { unsafe {
            let _ = Rc::from_raw(ptr as *const RefCell<bool>);
        }}

        RawWakerVTable::new(clone, wake, wake_by_ref, drop)
    };

    let raw = RawWaker::new(Rc::into_raw(state) as _, &VTABLE);

    // Safety: check for safety...
    unsafe { Waker::from_raw(raw) }
}

#[derive(Debug)]
pub enum Pollable {
    Wasi(WasiPollable),
}

impl Pollable {
    pub fn ready(&self) -> bool {
        match self {
            Pollable::Wasi(pollable) => pollable.ready(),
        }
    }
}

impl From<WasiPollable> for Pollable {
    fn from(pollable: WasiPollable) -> Self {
        Self::Wasi(pollable)
    }
}

#[derive(Clone)]
pub struct Reactor {
    inner: Rc<RefCell<InnerReactor>>,
}

type TaskInfo = (
    usize,
    Rc<RefCell<bool>>,
    Pin<Box<dyn Future<Output = ()> + 'static>>,
);

struct InnerReactor {
    next_id: usize,
    main_task_state: Rc<RefCell<bool>>,
    poller: Poller,
    wakers: HashMap<EventKey, Waker>,
    tasks: Vec<TaskInfo>,
    complete: HashMap<usize, Option<Waker>>,
}

impl Reactor {
    pub(crate) fn new() -> (Self, Waker) {
        let main_task_state = Rc::new(RefCell::new(true));
        (
            Self {
                inner: Rc::new(RefCell::new(InnerReactor {
                    next_id: 0,
                    main_task_state: main_task_state.clone(),
                    poller: Poller::new(),
                    wakers: HashMap::new(),
                    tasks: Vec::new(),
                    complete: HashMap::new(),
                })),
            },
            task_waker(main_task_state),
        )
    }

    #[instrument(skip_all)]
    pub async fn wait_for<P: Into<Pollable>>(&self, pollable: P) {
        let mut pollable = Some(pollable.into());
        let mut key = None;

        future::poll_fn(|cx| {
            let mut reactor = self.inner.borrow_mut();

            let key = key.get_or_insert_with(|| reactor.poller.insert(pollable.take().unwrap()));
            reactor.wakers.insert(*key, cx.waker().clone());

            if reactor.poller.get(key).unwrap().ready() {
                trace!("{key:?} is ready");
                reactor.poller.remove(*key);
                reactor.wakers.remove(key);
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    }

    #[instrument(skip_all)]
    pub(crate) fn block_until(&self) {
        let mut tasks = {
            let mut reactor = self.inner.borrow_mut();

            mem::take(&mut reactor.tasks)
        };

        let mut complete = HashSet::new();
        let mut pending = loop {
            let mut pending = vec![];
            while let Some((task_id, state, task)) = tasks.pop() {
                let Some((state, mut task)) = ({
                    let s = *state.borrow();
                    if s {
                        let state = state.clone();
                        *state.borrow_mut() = false;
                        Some((state, task))
                    } else {
                        pending.push((task_id, state, task));
                        None
                    }
                }) else {
                    continue;
                };

                let waker = task_waker(state.clone());
                let mut cx = Context::from_waker(&waker);

                if task.as_mut().poll(&mut cx).is_pending() {
                    pending.push((task_id, state, task));
                } else {
                    complete.insert(task_id);
                }
            }

            let ready = pending
                .iter()
                .filter(|(_, state, _)| *state.borrow())
                .count();

            trace!(
                "pending {:?} ready: {ready}",
                pending
                    .iter()
                    .map(|(task_id, ..)| task_id)
                    .collect::<Vec<_>>(),
            );

            if ready == 0 {
                break pending;
            }

            tasks = pending;
        };

        let mut reactor = self.inner.borrow_mut();
        reactor.tasks.append(&mut pending);
        for task_id in complete {
            if let Some(waker) = reactor.complete.get_mut(&task_id) {
                if let Some(waker) = waker.take() {
                    waker.wake_by_ref();
                }
            } else {
                reactor.complete.insert(task_id, None);
            }
        }

        if *reactor.main_task_state.borrow() {
            trace!("main task ready");
            return;
        }

        for key in reactor.poller.block_until() {
            trace!("wake key {key:?}");
            match reactor.wakers.get(&key) {
                Some(waker) => waker.wake_by_ref(),
                None => panic!("tried to wake the waker for non-existent `{key:?}`"),
            }
        }
    }

    pub(crate) fn reset_main_task_state(&self) {
        *self.inner.borrow_mut().main_task_state.borrow_mut() = false;
    }

    pub fn spawn(&self, f: impl Future<Output = ()> + 'static) -> JoinHandle {
        let mut reactor = self.inner.borrow_mut();

        let task_id = reactor.next_id;

        reactor.next_id += 1;

        reactor
            .tasks
            .push((task_id, Rc::new(RefCell::new(true)), Box::pin(f)));

        JoinHandle {
            reactor: self.clone(),
            task_id,
        }
    }
}

pub struct JoinHandle {
    reactor: Reactor,
    task_id: usize,
}

impl Future for JoinHandle {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut reactor = this.reactor.inner.borrow_mut();
        if reactor.complete.contains_key(&this.task_id) {
            Poll::Ready(())
        } else {
            reactor
                .complete
                .entry(this.task_id)
                .or_insert_with(|| Some(cx.waker().clone()));
            Poll::Pending
        }
    }
}
