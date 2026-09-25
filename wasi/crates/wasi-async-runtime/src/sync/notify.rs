use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::{self, Future};
use std::task::{Poll, Waker};

#[derive(Debug)]
struct NotifyInner {
    notified: bool,
    queue: VecDeque<Waker>,
}

#[derive(Debug)]
pub struct Notify {
    inner: RefCell<NotifyInner>,
}

impl Notify {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: RefCell::new(NotifyInner {
                notified: false,
                queue: VecDeque::new(),
            }),
        }
    }

    pub fn notify_one(&self) {
        // single thread
        let mut this = self.inner.borrow_mut();
        if !this.notified && let Some(waker) = this.queue.pop_front() {
            waker.wake();
        }

        this.notified = true;
    }

    pub fn notified(&self) -> impl Future<Output = ()> + '_ {
        future::poll_fn(move |cx| {
            let mut this = self.inner.borrow_mut();
            if this.notified {
                this.notified = false;
                Poll::Ready(())
            } else {
                this.queue.push_back(cx.waker().clone());
                Poll::Pending
            }
        })
    }
}

impl Default for Notify {
    fn default() -> Self {
        Self::new()
    }
}
