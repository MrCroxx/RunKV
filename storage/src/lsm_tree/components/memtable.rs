use crate::lsm_tree::utils::{full_key, raw_value, IterRef, KeyComparator, Skiplist};

#[derive(Clone)]
pub struct Comparator;

impl KeyComparator for Comparator {
    fn compare_key(&self, lhs: &[u8], rhs: &[u8]) -> std::cmp::Ordering {
        lhs.cmp(rhs)
    }

    fn same_key(&self, lhs: &[u8], rhs: &[u8]) -> bool {
        lhs == rhs
    }
}

pub struct Memtable {
    inner: Skiplist<Comparator>,
}

impl Memtable {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Skiplist::with_capacity(Comparator, capacity as u32),
        }
    }

    pub fn put(&mut self, key: &[u8], value: Option<&[u8]>, timestamp: u64) {
        let key = full_key(key, timestamp);
        self.inner.put(key, raw_value(value));
    }

    pub fn get(&self, key: &[u8], timestamp: u64) -> Option<&[u8]> {
        let key = full_key(key, timestamp);
        self.inner.get(&key).map(|k| &k[..])
    }

    pub(in crate::lsm_tree) fn iter(&self) -> IterRef<Skiplist<Comparator>, Comparator> {
        self.inner.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_send_sync<T: Send + Sync>() {}

    #[test]
    fn ensure_send_sync() {
        is_send_sync::<Memtable>()
    }
}
