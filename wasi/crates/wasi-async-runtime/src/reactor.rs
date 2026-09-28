use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use std::collections::{HashMap, VecDeque};

use wasi::io::poll::Pollable as WasiPollable;

use tracing::{instrument, trace};

use crate::poller::{EventKey, Poller};

type TaskId = usize;

/// Returns the [`Waker`]
///
/// # Note
///
/// Only valid for single thread environment
pub fn task_waker(state: Rc<RefCell<bool>>) -> Waker {
    const VTABLE: RawWakerVTable = {
        /// Clone the current data
        ///
        /// # Note
        ///
        /// Only valid for single thread environment
        unsafe fn clone(ptr: *const ()) -> RawWaker {
            unsafe {
                let ptr = ptr.cast::<RefCell<bool>>();

                // increment the strong counter for the current data
                Rc::increment_strong_count(ptr);

                RawWaker::new(ptr.cast(), &VTABLE)
            }
        }

        /// Wake the task
        ///
        /// Wake the task and consume the current data
        ///
        /// # Note
        ///
        /// Only valid for single thread environment
        unsafe fn wake(ptr: *const ()) {
            unsafe {
                // Recover the original [`Rc`] pointer
                let state = Rc::from_raw(ptr.cast::<RefCell<bool>>());
                if let Ok(state) = state.try_borrow_mut().as_mut() {
                    **state = true;
                }
            }
        }

        /// Wake the task by reference
        ///
        /// Wake the task by reference. This does *NOT* consume the current data
        ///
        /// # Note
        ///
        /// Only valid for single thread environment
        unsafe fn wake_by_ref(ptr: *const ()) {
            unsafe {
                let ptr = ptr.cast::<RefCell<bool>>();
                let state = ptr.as_ref_unchecked();
                if let Ok(state) = state.try_borrow_mut().as_mut() {
                    **state = true;
                }
            }
        }

        /// Wake the task
        ///
        /// Wake the task and consume the current data
        ///
        /// # Note
        ///
        /// Only valid for single thread environment
        unsafe fn drop(ptr: *const ()) {
            unsafe {
                // Recover the original [`Rc`] pointer
                // and let Rust to drop it as normal
                let _ = Rc::from_raw(ptr.cast::<RefCell<bool>>());
            }
        }

        RawWakerVTable::new(clone, wake, wake_by_ref, drop)
    };

    let raw = RawWaker::new(Rc::into_raw(state).cast(), &VTABLE);

    // SAFETY: the above assumptions are valid
    unsafe { Waker::from_raw(raw) }
}

#[derive(Debug)]
#[non_exhaustive]
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

type TaskInfo = (TaskId, Rc<RefCell<bool>>, Pin<Box<dyn Future<Output = ()>>>);

struct InnerReactor {
    next_id: TaskId,
    main_task_state: Rc<RefCell<bool>>,
    poller: Poller,
    wakers: HashMap<EventKey, Waker>,
    tasks: VecDeque<TaskInfo>,
    complete: HashMap<TaskId, (bool, Option<Waker>)>,
}

pub struct WaitFor<'a, P> {
    reactor: &'a Reactor,
    pollable: Option<P>,
    key: Option<EventKey>,
}

impl<'a, P> WaitFor<'a, P> {
    fn new(reactor: &'a Reactor, pollable: P) -> Self {
        Self {
            reactor,
            pollable: Some(pollable),
            key: None,
        }
    }
}

impl Future for WaitFor<'_, Pollable> {
    type Output = ();

    #[instrument(skip_all)]
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut reactor = this.reactor.inner.borrow_mut();

        let key = this.key.get_or_insert_with(|| {
            reactor.poller.insert(
                this.pollable
                    .take()
                    .expect("Invalid state: multi-thread env?"),
            )
        });
        reactor.wakers.insert(*key, cx.waker().clone());

        if reactor.poller.get(*key).unwrap().ready() {
            trace!("{key:?} is ready");
            reactor.poller.remove(*key);
            reactor.wakers.remove(key);
            this.key = None;
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl<P> Drop for WaitFor<'_, P> {
    #[instrument(skip_all)]
    fn drop(&mut self) {
        if let Some(key) = self.key {
            trace!("Dropped {key:?}");
            let mut reactor = self.reactor.inner.borrow_mut();
            reactor.poller.remove(key);
            reactor.wakers.remove(&key);
        }
    }
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
                    tasks: VecDeque::new(),
                    complete: HashMap::new(),
                })),
            },
            task_waker(main_task_state),
        )
    }

    #[instrument(skip_all)]
    pub fn wait_for<P: Into<Pollable>>(&self, pollable: P) -> WaitFor<'_, Pollable> {
        WaitFor::new(self, pollable.into())
    }

    #[instrument(skip_all)]
    pub(crate) fn block_until(&self) {
        trace!("block_util tasks len: {}", self.inner.borrow().tasks.len());
        while self
            .inner
            .borrow()
            .tasks
            .iter()
            .any(|(_, state, _)| *state.borrow())
        {
            let mut pending = self.inner.borrow().tasks.len();
            while pending != 0 {
                let Some((task_id, current_state, mut task)) =
                    self.inner.borrow_mut().tasks.pop_front()
                else {
                    break;
                };
                pending -= 1;

                if *current_state.borrow() {
                    // the task is pollable
                    let state = current_state.clone();

                    *state.borrow_mut() = false;

                    trace!("poll task id {task_id}");
                    let waker = task_waker(state.clone());
                    let mut cx = Context::from_waker(&waker);

                    let original_len = self.inner.borrow().tasks.len();
                    let is_pending = task.as_mut().poll(&mut cx).is_pending();
                    let current_len = self.inner.borrow().tasks.len();

                    pending += current_len - original_len;

                    if is_pending {
                        self.inner
                            .borrow_mut()
                            .tasks
                            .push_back((task_id, state, task));
                    } else if let Some((complete, waker)) =
                        self.inner.borrow_mut().complete.get_mut(&task_id)
                    {
                        *complete = true;
                        if let Some(waker) = waker {
                            waker.wake_by_ref();
                        }
                    }
                } else {
                    self.inner
                        .borrow_mut()
                        .tasks
                        .push_back((task_id, current_state, task));
                }
            }
        }

        let mut reactor = self.inner.borrow_mut();

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
        let task_id = {
            // limit borrow mut span
            let mut reactor = self.inner.borrow_mut();

            let task_id = reactor.next_id;

            reactor.next_id += 1;

            reactor
                .tasks
                .push_back((task_id, Rc::new(RefCell::new(true)), Box::pin(f)));

            task_id
        };

        JoinHandle::new(self.clone(), task_id)
    }
}

pub struct JoinHandle {
    reactor: Reactor,
    task_id: TaskId,
}

impl JoinHandle {
    fn new(reactor: Reactor, task_id: TaskId) -> Self {
        reactor
            .inner
            .borrow_mut()
            .complete
            .insert(task_id, (false, None));

        Self { reactor, task_id }
    }
}

impl Future for JoinHandle {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut reactor = this.reactor.inner.borrow_mut();

        let (complete, waker) = reactor.complete.get_mut(&this.task_id).unwrap();
        if *complete {
            Poll::Ready(())
        } else {
            *waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl Drop for JoinHandle {
    fn drop(&mut self) {
        self.reactor
            .inner
            .borrow_mut()
            .complete
            .remove(&self.task_id);
    }
}
