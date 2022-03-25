#[macro_export]
macro_rules! may_advance_atomic {
    ($atomic:expr, $val:expr) => {
        let mut old = $atomic.load(Ordering::Relaxed);
        while $val > old {
            match $atomic.compare_exchange_weak(old, $val, Ordering::SeqCst, Ordering::Relaxed) {
                Ok(_) => break,
                Err(v) => old = v,
            }
        }
    };
}
