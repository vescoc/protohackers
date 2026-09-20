use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};

use tracing::{instrument, trace};

use crate::sync::notify::Notify;
use crate::sync::semaphore::{Semaphore, SemaphorePermit};

#[derive(Debug)]
struct RwLockInner<T> {
    readers: usize,
    value: T,
}

#[derive(Debug)]
pub struct RwLock<T> {
    writer: Semaphore,
    notify: Notify,
    inner: UnsafeCell<RwLockInner<T>>,
}

impl<T> RwLock<T> {
    pub fn new(value: T) -> Self {
        let notify = Notify::new();
        notify.notify_one();

        Self {
            writer: Semaphore::new(1),
            notify,
            inner: UnsafeCell::new(RwLockInner { readers: 0, value }),
        }
    }

    #[instrument(skip_all)]
    pub async fn write(&self) -> RwLockWriteGuard<'_, T> {
        trace!("write");
        let permit = self.writer.acquire().await;

        if unsafe { &*self.inner.get() }.readers > 0 {
            trace!("waiting notified");
            self.notify.notified().await;
        }

        debug_assert_eq!(unsafe { &*self.inner.get() }.readers, 0);

        trace!("ok");
        RwLockWriteGuard {
            _permit: permit,
            value: &mut unsafe { &mut *self.inner.get() }.value,
        }
    }

    #[instrument(skip_all)]
    pub async fn read(&self) -> RwLockReadGuard<'_, T> {
        trace!("read");
        let _ = self.writer.acquire().await;

        let readers = &mut unsafe { &mut *self.inner.get() }.readers;
        *readers += 1;

        RwLockReadGuard {
            value: &unsafe { &*self.inner.get() }.value,
            notify: &self.notify,
            readers,
        }
    }
}

#[derive(Debug)]
pub struct RwLockWriteGuard<'a, T> {
    _permit: SemaphorePermit<'a>,
    value: &'a mut T,
}

impl<T> Deref for RwLockWriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.value
    }
}

impl<T> DerefMut for RwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value
    }
}

#[derive(Debug)]
pub struct RwLockReadGuard<'a, T> {
    value: &'a T,
    notify: &'a Notify,
    readers: &'a mut usize,
}

impl<T> Deref for RwLockReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.value
    }
}

impl<T> Drop for RwLockReadGuard<'_, T> {
    fn drop(&mut self) {
        *self.readers -= 1;
        if *self.readers == 0 {
            self.notify.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::sync::Once;

    use futures_concurrency::future::Join;

    use crate::block_on;

    use super::*;

    fn init_tracing_subscriber() {
        static INIT_TRACING_SUBSCRIBER: Once = Once::new();
        INIT_TRACING_SUBSCRIBER.call_once(tracing_subscriber::fmt::init);
    }

    #[test]
    fn test_rw_lock() {
        init_tracing_subscriber();

        block_on(|reactor| async move {
            let rw = Rc::new(RwLock::new(0));

            let handle_1 = {
                let rw = rw.clone();
                reactor.spawn(async move {
                    let value = &*rw.read().await;

                    trace!("handle_1: {value}");

                    assert!((0..=2).contains(value));
                })
            };

            let handle_2 = {
                let rw = rw.clone();
                reactor.spawn(async move {
                    let value = &*rw.read().await;

                    trace!("handle_2: {value}");

                    assert!((0..=2).contains(value));
                })
            };

            let handle_3 = {
                let rw = rw.clone();
                reactor.spawn(async move {
                    let value = &mut *rw.write().await;

                    *value += 1;

                    trace!("handle_3: {value}");
                })
            };

            let handle_5 = {
                let rw = rw.clone();
                reactor.spawn(async move {
                    let value = &mut *rw.write().await;

                    *value += 1;
                    
                    trace!("handle_5: {value}");
                })
            };

            let handle_4 = {
                let rw = rw.clone();
                reactor.spawn(async move {
                    let value = &*rw.read().await;

                    trace!("handle_4: {value}");

                    assert!((0..=2).contains(value));
                })
            };

            (handle_1, handle_2, handle_3, handle_4, handle_5).join().await
        });
    }
}
