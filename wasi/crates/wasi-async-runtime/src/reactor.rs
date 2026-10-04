use std::cell::RefCell;
use std::future::Future;
use std::pin::{self, Pin};
use std::rc::Rc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use std::collections::{HashMap, VecDeque};

use wasi::io::poll::Pollable as WasiPollable;

use tracing::{instrument, trace, warn};

use crate::poller::{EventKey, Poller};

type TaskId = usize;

struct TaskStateInternal {
    task_id: TaskId,
    reactor: Reactor,
}

impl TaskStateInternal {
    fn new(task_id: TaskId, reactor: Reactor) -> Rc<Self> {
        Rc::new(Self { task_id, reactor })
    }
}

type TaskState = Rc<TaskStateInternal>;

/// Returns the [`Waker`]
///
/// # Note
///
/// Only valid for single thread environment
fn task_waker(running: bool, state: TaskState) -> Waker {
    const VTABLE: RawWakerVTable = {
        /// Clone the current data
        ///
        /// # Note
        ///
        /// Only valid for single thread environment
        unsafe fn clone(ptr: *const ()) -> RawWaker {
            unsafe {
                let ptr = ptr.cast::<TaskStateInternal>();

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
                let state = Rc::from_raw(ptr.cast::<TaskStateInternal>());
                state
                    .reactor
                    .inner
                    .borrow_mut()
                    .running
                    .push_back((false, state.task_id));
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
                let ptr = ptr.cast::<TaskStateInternal>();
                let state = ptr.as_ref_unchecked();
                state
                    .reactor
                    .inner
                    .borrow_mut()
                    .running
                    .push_back((false, state.task_id));
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
                let _ = Rc::from_raw(ptr.cast::<TaskStateInternal>());
            }
        }

        RawWakerVTable::new(clone, wake, wake_by_ref, drop)
    };

    if running {
        let mut reactor = state.reactor.inner.borrow_mut();
        reactor.running.push_back((false, state.task_id));
    }

    let raw = RawWaker::new(Rc::into_raw(state).cast(), &VTABLE);

    // SAFETY: the above assumptions are valid
    unsafe { Waker::from_raw(raw) }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum Pollable {
    Wasi(bool, WasiPollable),
}

impl Pollable {
    pub fn ready(&self) -> bool {
        match self {
            Pollable::Wasi(error, pollable) => *error || pollable.ready(),
        }
    }
}

impl From<WasiPollable> for Pollable {
    fn from(pollable: WasiPollable) -> Self {
        Self::Wasi(false, pollable)
    }
}

#[derive(Clone)]
pub struct Reactor {
    inner: Rc<RefCell<InnerReactor>>,
}

type TaskInfo = Pin<Box<dyn Future<Output = ()>>>;

struct InnerReactor {
    next_id: TaskId,
    poller: Poller,
    running: VecDeque<(bool, TaskId)>,
    tasks: HashMap<TaskId, TaskInfo>,
    complete: HashMap<TaskId, (bool, Option<Waker>)>,
    wakers: HashMap<EventKey, Waker>,
}

impl InnerReactor {
    fn next_id(&mut self) -> usize {
        let task_id = self.next_id;
        self.next_id += 1;
        task_id
    }
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
            trace!("{key:?} is pending");
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
    pub fn current() -> impl Future<Output = Self> {
        std::future::poll_fn(|cx| {
            let data = cx.waker().data().cast::<TaskStateInternal>();
            // SAFETY: The context is valid, so the data is valid
            let TaskStateInternal {
                task_id: _,
                reactor,
            } = unsafe { data.as_ref_unchecked() };
            Poll::Ready(reactor.clone())
        })
    }

    #[instrument(skip_all)]
    pub fn wait_for<P: Into<Pollable>>(&self, pollable: P) -> WaitFor<'_, Pollable> {
        WaitFor::new(self, pollable.into())
    }

    #[instrument(skip_all)]
    pub fn block_on<F, Fut>(f: F) -> Fut::Output
    where
        F: FnOnce(Self) -> Fut,
        Fut: Future,
    {
        let main_task_id = 0;
        let tasks: HashMap<TaskId, TaskInfo> = HashMap::new();

        let reactor = Self {
            inner: Rc::new(RefCell::new(InnerReactor {
                next_id: main_task_id + 1,
                poller: Poller::new(),
                running: VecDeque::new(),
                tasks,
                complete: HashMap::new(),
                wakers: HashMap::new(),
            })),
        };
        let main_task_state = TaskStateInternal::new(main_task_id, reactor.clone());

        let main = (f)(reactor.clone());
        let mut main = pin::pin!(main);

        reactor
            .inner
            .borrow_mut()
            .running
            .push_back((false, main_task_id));

        loop {
            let running_len = reactor.inner.borrow().running.len();

            trace!(
                "tasks len: {} running: {running_len}",
                reactor.inner.borrow().tasks.len(),
            );

            for (polled, _) in &mut reactor.inner.borrow_mut().running {
                *polled = false;
            }

            let mut complete_wakers = Vec::with_capacity(running_len);
            while let Some((polled, task_id)) = { reactor.inner.borrow_mut().running.pop_front() } {
                trace!("poll task id {task_id} polled: {polled}");
                if polled {
                    trace!("already polled {task_id}");
                    reactor
                        .inner
                        .borrow_mut()
                        .running
                        .push_back((false, task_id));
                    break;
                }

                let Some(mut task) = reactor.inner.borrow_mut().tasks.remove(&task_id) else {
                    if task_id == 0 {
                        trace!("running main task");

                        let waker = task_waker(false, main_task_state.clone());
                        let mut cx = Context::from_waker(&waker);

                        if let Poll::Ready(result) = main.as_mut().poll(&mut cx) {
                            trace!("main task done");
                            return result;
                        }
                    } else {
                        warn!(
                            "cannot find task {task_id}, is complete? {}",
                            complete_wakers
                                .iter()
                                .any(|(completed_task_id, _)| task_id == *completed_task_id)
                        );
                    }
                    continue;
                };

                let waker = task_waker(false, TaskStateInternal::new(task_id, reactor.clone()));
                let mut cx = Context::from_waker(&waker);

                if task.as_mut().poll(&mut cx).is_pending() {
                    reactor.inner.borrow_mut().tasks.insert(task_id, task);
                } else if let Some((complete, waker)) =
                    reactor.inner.borrow_mut().complete.get_mut(&task_id)
                {
                    *complete = true;
                    if let Some(waker) = waker {
                        complete_wakers.push((task_id, waker.clone()));
                    }
                }
            }

            for (task_id, waker) in complete_wakers {
                trace!("wake complete {task_id}");
                waker.wake();
            }

            trace!(
                "block_on before poller tasks len: {} running: {}",
                reactor.inner.borrow().tasks.len(),
                reactor.inner.borrow().running.len()
            );

            {
                let mut reactor = reactor.inner.borrow_mut();
                let poller_wakers = reactor
                    .poller
                    .block_until()
                    .iter()
                    .map(|key| (*key, reactor.wakers[key].clone()))
                    .collect::<Vec<_>>();
                drop(reactor);

                for (key, waker) in poller_wakers {
                    trace!("wake poller {key:?}");
                    waker.wake();
                }
            }
        }
    }

    #[instrument(skip_all)]
    pub fn spawn<F>(&self, f: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
    {
        let result = Rc::new(RefCell::new(None));
        let task_id = {
            // limit borrow mut span
            let result = Rc::clone(&result);
            let task = async move {
                *result.borrow_mut() = Some(f.await);
            };

            let mut reactor = self.inner.borrow_mut();

            let task_id = reactor.next_id();

            reactor.tasks.insert(task_id, Box::pin(task));
            reactor.running.push_back((false, task_id));

            task_id
        };

        trace!("spawned task id {task_id}");

        JoinHandle::new(self.clone(), task_id, result)
    }

    pub async fn spawn_in_current<F>(f: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
    {
        Self::current().await.spawn(f)
    }
}

pub struct JoinHandle<T> {
    reactor: Reactor,
    task_id: TaskId,
    result: Rc<RefCell<Option<T>>>,
}

impl<T> JoinHandle<T> {
    fn new(reactor: Reactor, task_id: TaskId, result: Rc<RefCell<Option<T>>>) -> Self {
        reactor
            .inner
            .borrow_mut()
            .complete
            .insert(task_id, (false, None));

        Self {
            reactor,
            task_id,
            result,
        }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut reactor = this.reactor.inner.borrow_mut();

        let (complete, waker) = reactor.complete.get_mut(&this.task_id).unwrap();
        if *complete {
            Poll::Ready(this.result.borrow_mut().take().unwrap())
        } else {
            *waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        self.reactor
            .inner
            .borrow_mut()
            .complete
            .remove(&self.task_id);
    }
}
