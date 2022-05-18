#![allow(dead_code)]
#![allow(clippy::declare_interior_mutable_const)]

use tracing::trace;

// parking_lot

pub struct TracedParkingLogRawMutex<const H: &'static str, const N: &'static str> {
    inner: parking_lot::RawMutex,
}

impl<const H: &'static str, const N: &'static str> TracedParkingLogRawMutex<H, N> {
    const fn const_new() -> Self {
        Self {
            inner: <parking_lot::RawMutex as parking_lot::lock_api::RawMutex>::INIT,
        }
    }
}

unsafe impl<const H: &'static str, const N: &'static str> parking_lot::lock_api::RawMutex
    for TracedParkingLogRawMutex<H, N>
{
    const INIT: Self = Self::const_new();

    type GuardMarker = parking_lot::lock_api::GuardNoSend;

    fn lock(&self) {
        trace!(holder = H, name = N, "lock",);
        self.inner.lock()
    }

    fn try_lock(&self) -> bool {
        trace!(holder = H, name = N, "try lock",);
        self.inner.try_lock()
    }

    unsafe fn unlock(&self) {
        trace!(holder = H, name = N, "unlock",);
        self.inner.unlock()
    }
}

pub type TracedParkingLotMutex<T, const H: &'static str, const N: &'static str> =
    parking_lot::lock_api::Mutex<TracedParkingLogRawMutex<H, N>, T>;

pub struct TracedParkingLogRawRwLock<const H: &'static str, const N: &'static str> {
    inner: parking_lot::RawRwLock,
}

impl<const H: &'static str, const N: &'static str> TracedParkingLogRawRwLock<H, N> {
    const fn const_new() -> Self {
        Self {
            inner: <parking_lot::RawRwLock as parking_lot::lock_api::RawRwLock>::INIT,
        }
    }
}

unsafe impl<const H: &'static str, const N: &'static str> parking_lot::lock_api::RawRwLock
    for TracedParkingLogRawRwLock<H, N>
{
    const INIT: Self = Self::const_new();

    type GuardMarker = parking_lot::lock_api::GuardNoSend;

    fn lock_shared(&self) {
        trace!(holder = H, name = N, "lock shared",);
        self.inner.lock_shared()
    }

    fn try_lock_shared(&self) -> bool {
        trace!(holder = H, name = N, "try lock shared",);
        self.inner.try_lock_shared()
    }

    unsafe fn unlock_shared(&self) {
        trace!(holder = H, name = N, "unlock shared",);
        self.inner.unlock_shared()
    }

    fn lock_exclusive(&self) {
        trace!(holder = H, name = N, "lock exclusive",);
        self.inner.lock_exclusive()
    }

    fn try_lock_exclusive(&self) -> bool {
        trace!(holder = H, name = N, "try lock exclusive",);
        self.inner.try_lock_exclusive()
    }

    unsafe fn unlock_exclusive(&self) {
        trace!(holder = H, name = N, "unlock exclusive",);
        self.inner.unlock_exclusive()
    }
}

pub type TracedParkingLotRwLock<T, const H: &'static str, const N: &'static str> =
    parking_lot::lock_api::RwLock<TracedParkingLogRawRwLock<H, N>, T>;

// tokio

pub struct TracedTokioMutexGuard<'a, T> {
    inner: tokio::sync::MutexGuard<'a, T>,
}

pub struct TracedTokioMutex<T> {
    inner: tokio::sync::Mutex<T>,
}

impl<T> TracedTokioMutex<T> {
    pub fn new(data: T) -> Self {
        Self {
            inner: tokio::sync::Mutex::new(data),
        }
    }

    pub async fn lock() -> TracedTokioMutexGuard<'_, T> {
        todo!()
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use parking_lot::Mutex;
    use runkv_proc::trace_lock;
    use test_log::test;
    use tokio::sync::RwLock;

    use super::*;

    #[trace_lock(parking_lot, AsyncMutex = TokioMutex)]
    struct S {
        core: Arc<Mutex<usize>>,
    }

    struct T<const H: &'static str> {}

    impl<const H: &'static str> T<H> {
        fn test() {
            trace!("H: {}", H);
        }
    }

    #[test]
    fn test() {
        let mutex = TracedParkingLotMutex::<usize, "t_holder", "t_name">::new(0);
        let guard = mutex.lock();
        drop(guard);

        let rwlock = TracedParkingLotRwLock::<usize, "t_holder", "t_name">::new(0);
        let guard = rwlock.read();
        drop(guard);
        let guard = rwlock.write();
        drop(guard);

        T::<"haha">::test();
    }
}
