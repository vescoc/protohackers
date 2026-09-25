use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};

use tracing::{instrument, trace};

use crate::sync::notify::Notify;
use crate::sync::semaphore::{Semaphore, SemaphorePermit};

#[derive(Debug)]
pub struct RwLock<T> {
    lock: Semaphore,
    notify_writers: Notify,
    readers: UnsafeCell<usize>,
    value: UnsafeCell<T>,
}

impl<T> RwLock<T> {
    pub fn new(value: T) -> Self {
        let notify_writers = Notify::new();
        notify_writers.notify_one();

        Self {
            lock: Semaphore::new(1),
            notify_writers,
            readers: UnsafeCell::new(0),
            value: UnsafeCell::new(value),
        }
    }

    #[instrument(skip_all)]
    pub async fn write(&self) -> RwLockWriteGuard<'_, T> {
        trace!("write");
        let permit = self.lock.acquire().await;

        if unsafe { *self.readers.get() } > 0 {
            trace!("waiting notified");
            self.notify_writers.notified().await;
        }

        debug_assert_eq!(unsafe { *self.readers.get() }, 0);

        trace!("ok");
        RwLockWriteGuard {
            _permit: permit,
            value: &self.value,
        }
    }

    #[instrument(skip_all)]
    pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
        trace!("try_write");
        
        let permit = self.lock.try_acquire()?;

        if unsafe { *self.readers.get() } > 0 {
            return None;
        }

        debug_assert_eq!(unsafe { *self.readers.get() }, 0);

        trace!("ok");
        Some(RwLockWriteGuard {
            _permit: permit,
            value: &self.value,
        })
    }

    #[instrument(skip_all)]
    pub async fn read(&self) -> RwLockReadGuard<'_, T> {
        trace!("read");
        let _ = self.lock.acquire().await;

        let readers = unsafe { &mut *self.readers.get() };
        *readers += 1;

        RwLockReadGuard {
            value: &self.value,
            notify_writers: &self.notify_writers,
            readers: &self.readers,
        }
    }
    
    #[instrument(skip_all)]
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        trace!("try_read");
        let _ = self.lock.try_acquire()?;

        let readers = unsafe { &mut *self.readers.get() };
        *readers += 1;

        Some(RwLockReadGuard {
            value: &self.value,
            notify_writers: &self.notify_writers,
            readers: &self.readers,
        })
    }
}

#[derive(Debug)]
pub struct RwLockWriteGuard<'a, T> {
    _permit: SemaphorePermit<'a>,
    value: &'a UnsafeCell<T>,
}

impl<T> Deref for RwLockWriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.value.get() }
    }
}

impl<T> DerefMut for RwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.value.get() }
    }
}

#[derive(Debug)]
pub struct RwLockReadGuard<'a, T> {
    value: &'a UnsafeCell<T>,
    notify_writers: &'a Notify,
    readers: &'a UnsafeCell<usize>,
}

impl<T> Deref for RwLockReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.value.get() }
    }
}

impl<T> Drop for RwLockReadGuard<'_, T> {
    fn drop(&mut self) {
        *(unsafe { &mut *self.readers.get() }) -= 1;
        if unsafe { *self.readers.get() } == 0 {
            self.notify_writers.notify_one();
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
    fn test_read_lock() {
        init_tracing_subscriber();

        block_on(|_| async move {
            let lock = RwLock::new(0);

            let read_lock_1 = lock.read().await;
            assert_eq!(*read_lock_1, 0);

            let read_lock_2 = lock.read().await;
            assert_eq!(*read_lock_1, 0);
            assert_eq!(*read_lock_2, 0);
            assert_eq!(*read_lock_1, 0);
        });
    }

    #[test]
    fn test_try_read_lock() {
        init_tracing_subscriber();

        let lock = RwLock::new(0);

        let read_lock_1 = lock.try_read().unwrap();
        assert_eq!(*read_lock_1, 0);

        let read_lock_2 = lock.try_read().unwrap();
        assert_eq!(*read_lock_1, 0);
        assert_eq!(*read_lock_2, 0);
    }    

    #[test]
    fn test_try_write_lock() {
        init_tracing_subscriber();

        let lock = RwLock::new(0);

        {
            let mut write_lock = lock.try_write().unwrap();
            *write_lock = 1;
        }

        {
            let read_lock = lock.try_read().unwrap();
            assert_eq!(*read_lock, 1);

            assert!(lock.try_write().is_none());
        }
    }    

    #[test]
    fn test_try_write_lock_multi() {
        init_tracing_subscriber();

        let lock = RwLock::new(0);

        {
            let mut write_lock = lock.try_write().unwrap();
            *write_lock = 1;

            assert!(lock.try_write().is_none());
        }

        {
            let read_lock = lock.try_read().unwrap();
            assert_eq!(*read_lock, 1);

            assert!(lock.try_write().is_none());
        }
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
