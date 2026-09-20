use std::cell::RefCell;
use std::future::{self, Future};
use std::task::{Poll, Waker};

#[derive(Debug)]
struct SemaphoreInner {
    permits: usize,
    queue: Vec<Waker>,
}

#[derive(Debug)]
pub struct Semaphore {
    inner: RefCell<SemaphoreInner>,
}

impl Semaphore {
    pub fn new(permits: usize) -> Self {
        Self {
            inner: RefCell::new(SemaphoreInner {
                permits,
                queue: vec![],
            }),
        }
    }

    pub fn acquire(&self) -> impl Future<Output = SemaphorePermit<'_>> {
        future::poll_fn(move |cx| {
            let mut this = self.inner.borrow_mut();
            if this.permits == 0 {
                this.queue.push(cx.waker().clone());
                Poll::Pending
            } else {
                this.permits -= 1;
                Poll::Ready(SemaphorePermit { this: self })
            }
        })
    }
}

#[derive(Debug)]
pub struct SemaphorePermit<'a> {
    this: &'a Semaphore,
}

impl Drop for SemaphorePermit<'_> {
    fn drop(&mut self) {
        // single thread
        let mut this = self.this.inner.borrow_mut();

        this.permits += 1;

        for waker in this.queue.drain(..) {
            waker.wake();
        }
    }
}
