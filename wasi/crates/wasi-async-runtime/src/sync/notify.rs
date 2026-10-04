use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::{self, Future};
use std::task::{Poll, Waker};

#[derive(Debug)]
struct NotifyInner {
    notified: usize,
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
                notified: 0,
                queue: VecDeque::new(),
            }),
        }
    }

    pub fn notify_one(&self) {
        // single thread
        let mut this = self.inner.borrow_mut();
        if this.notified == 0
            && let Some(waker) = this.queue.pop_front()
        {
            this.notified += 1;
            waker.wake();
        }
    }

    pub fn notify_waiters(&self) {
        // single thread
        let mut this = self.inner.borrow_mut();
        if this.notified == 0 {
            while let Some(waker) = this.queue.pop_front() {
                this.notified += 1;
                waker.wake();
            }
        }
    }

    pub fn notified(&self) -> impl Future<Output = ()> + '_ {
        future::poll_fn(move |cx| {
            let mut this = self.inner.borrow_mut();
            if this.notified > 0 {
                this.notified -= 1;
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

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use crate::tests::init_tracing_subscriber;
    use crate::{Reactor, yield_now};

    use super::*;

    #[test]
    fn test_notify_one() {
        init_tracing_subscriber();

        Reactor::block_on(|_| async move {
            let value = Rc::new(RefCell::new(false));
            let lock = Rc::new(Notify::new());
            let handle = {
                let value = Rc::clone(&value);
                let lock = Rc::clone(&lock);
                Reactor::spawn_in_current(async move {
                    assert!(!*value.borrow());
                    lock.notified().await;
                    *value.borrow_mut() = true;
                })
                .await
            };

            yield_now().await;
            assert!(!*value.borrow());

            lock.notify_one();
            handle.await;

            assert!(*value.borrow());
        });
    }

    #[test]
    fn test_notify_waiters() {
        init_tracing_subscriber();

        Reactor::block_on(|_| async move {
            let value = Rc::new(RefCell::new(0));
            let lock = Rc::new(Notify::new());
            let handle1 = {
                let value = Rc::clone(&value);
                let lock = Rc::clone(&lock);
                Reactor::spawn_in_current(async move {
                    assert_eq!(*value.borrow(), 0);
                    lock.notified().await;
                    *value.borrow_mut() += 100;
                })
                .await
            };
            let handle2 = {
                let value = Rc::clone(&value);
                let lock = Rc::clone(&lock);
                Reactor::spawn_in_current(async move {
                    assert_eq!(*value.borrow(), 0);
                    lock.notified().await;
                    *value.borrow_mut() += 100;
                })
                .await
            };

            yield_now().await;
            assert_eq!(*value.borrow(), 0);

            lock.notify_waiters();
            handle1.await;
            handle2.await;

            assert_eq!(*value.borrow(), 200);

            assert_eq!(lock.inner.borrow().notified, 0);
            assert!(lock.inner.borrow().queue.is_empty());
        });
    }
}
