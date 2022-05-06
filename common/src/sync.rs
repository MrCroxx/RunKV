use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct TicketLockCore {
    head: AtomicUsize,
    tail: AtomicUsize,
}

#[derive(Clone)]
pub struct TicketLock {
    core: Arc<TicketLockCore>,
}

impl Default for TicketLock {
    fn default() -> Self {
        Self {
            core: Arc::new(TicketLockCore {
                head: AtomicUsize::new(0),
                tail: AtomicUsize::new(0),
            }),
        }
    }
}

impl TicketLock {
    pub fn acquire(&self) -> usize {
        let ticket = self.core.head.fetch_add(1, Ordering::SeqCst);
        while ticket != self.core.tail.load(Ordering::Acquire) {}
        ticket
    }

    pub async fn async_acquire(&self) -> usize {
        let ticket = self.core.head.fetch_add(1, Ordering::SeqCst);
        while ticket != self.core.tail.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        ticket
    }

    pub fn release(&self) {
        self.core.tail.fetch_add(1, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use itertools::Itertools;
    use parking_lot::Mutex;
    use rand::Rng;
    use test_log::test;

    use super::*;

    #[test]
    fn test_ticket_lock() {
        let lock = TicketLock::default();
        loop {
            let results = Arc::new(Mutex::new(vec![]));

            let handles = (0..100)
                .into_iter()
                .map(|_| {
                    let lock_clone = lock.clone();
                    let results_clone = results.clone();
                    std::thread::spawn(move || {
                        let mut rng = rand::thread_rng();
                        std::thread::sleep(Duration::from_millis(rng.gen_range(10..100)));
                        let ticket = lock_clone.acquire();
                        results_clone.lock().push(ticket);
                        lock_clone.release();
                        ticket
                    })
                })
                .collect_vec();

            let tickets = handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect_vec();
            let mut ordered = true;
            for (i, t) in tickets.into_iter().enumerate() {
                if i != t {
                    ordered = false;
                    break;
                }
            }
            if ordered {
                continue;
            }
            let results = Arc::try_unwrap(results).unwrap().into_inner();
            for (i, r) in results.into_iter().enumerate() {
                assert_eq!(i, r);
            }
            break;
        }
    }

    #[test(tokio::test(flavor = "multi_thread", worker_threads = 10))]
    async fn test_ticket_lock_async() {
        let lock = TicketLock::default();
        loop {
            let results = Arc::new(Mutex::new(vec![]));

            let futures = (0..100)
                .into_iter()
                .map(|_| {
                    let lock_clone = lock.clone();
                    let results_clone = results.clone();
                    async move {
                        let mut rng = rand::thread_rng();
                        tokio::time::sleep(Duration::from_millis(rng.gen_range(10..100))).await;
                        let ticket = lock_clone.async_acquire().await;
                        results_clone.lock().push(ticket);
                        lock_clone.release();
                        ticket
                    }
                })
                .collect_vec();
            let tickets = futures::future::join_all(futures).await;
            let mut ordered = true;
            for (i, t) in tickets.into_iter().enumerate() {
                if i != t {
                    ordered = false;
                    break;
                }
            }
            if ordered {
                continue;
            }
            let results = Arc::try_unwrap(results).unwrap().into_inner();
            for (i, r) in results.into_iter().enumerate() {
                assert_eq!(i, r);
            }
            break;
        }
    }
}
