use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};

use crate::sync::semaphore::{Semaphore, SemaphorePermit};

#[derive(Debug)]
pub struct Mutex<T> {
    semaphore: Semaphore,
    value: UnsafeCell<T>,
}

impl<T> Mutex<T> {
    pub fn new(value: T) -> Self {
        Self {
            semaphore: Semaphore::new(1),
            value: UnsafeCell::new(value),
        }
    }

    pub async fn lock(&self) -> MutexGuard<'_, T> {
        let permit = self.semaphore.acquire().await;

        // SAFETY: protect by a semaphore
        MutexGuard::new(permit, unsafe { &mut *self.value.get() })
    }
}

#[derive(Debug)]
pub struct MutexGuard<'a, T> {
    _permit: SemaphorePermit<'a>,
    value: &'a mut T,
}

impl<'a, T> MutexGuard<'a, T> {
    fn new(permit: SemaphorePermit<'a>, value: &'a mut T) -> Self {
        Self {
            _permit: permit,
            value,
        }
    }
}

impl<T> Deref for MutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.value
    }
}

impl<T> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use futures_concurrency::future::Join;

    use crate::block_on;

    use super::*;

    #[test]
    fn test_mutex() {
        block_on(|reactor| async move {
            let mutex = Rc::new(Mutex::new(0));

            let handle_1 = {
                let mutex = mutex.clone();
                reactor.spawn(async move {
                    *mutex.lock().await += 1;
                })
            };

            let handle_2 = {
                let mutex = mutex.clone();
                reactor.spawn(async move {
                    *mutex.lock().await += 1;
                })
            };

            (handle_1, handle_2).join().await;

            assert_eq!(2, *mutex.lock().await);
        });
    }
}
