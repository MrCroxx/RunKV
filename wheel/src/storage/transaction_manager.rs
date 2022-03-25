use std::collections::btree_map::{BTreeMap, Entry};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::error::{Result, TransactionError};

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

pub struct TransactionManager {
    txn: AtomicU32,
    timestamp: AtomicU32,
    watermark: AtomicU64,
    pinned_tts: RwLock<BTreeMap<u64, u64>>,
}

impl TransactionManager {
    /// `txn` should be a new transaction id that never written with.
    pub fn new(txn: u32) -> Self {
        Self {
            txn: AtomicU32::new(txn),
            timestamp: AtomicU32::new(0),
            watermark: AtomicU64::new((txn as u64) << 32),
            pinned_tts: RwLock::new(BTreeMap::default()),
        }
    }

    /// Get the current `tts` (transaction id and timestamp).
    pub fn tts(&self) -> u64 {
        ((self.txn.load(Ordering::SeqCst) as u64) << 32)
            | (self.timestamp.load(Ordering::SeqCst) as u64)
    }

    /// Get stored watermark.
    pub fn watermark(&self) -> u64 {
        self.watermark.load(Ordering::Relaxed)
    }

    /// Advance stored watermark to a new safe point.
    pub fn may_advance_watermark(&self) {
        let guard = self.pinned_tts.read();
        let watermark = match guard.first_key_value() {
            Some((min_tts, _)) => *min_tts,
            None => self.tts(),
        };
        drop(guard);
        may_advance_atomic!(self.watermark, watermark);
    }

    /// Pin `tts` to prevent `watermark` from advancing above it.
    pub fn pin_tts(&self, tts: u64) -> Result<()> {
        let watermark = self.watermark.load(Ordering::Relaxed);
        if watermark > tts {
            return Err(TransactionError::OutdatedTts(tts, watermark).into());
        }
        let mut guard = self.pinned_tts.write();
        match guard.entry(tts) {
            Entry::Vacant(v) => {
                v.insert(1);
            }
            Entry::Occupied(mut o) => {
                *o.get_mut() += 1;
            }
        }
        Ok(())
    }

    /// Unpin `tts`.
    pub fn unpin_tts(&self, tts: u64) -> Result<()> {
        let mut guard = self.pinned_tts.write();
        match guard.entry(tts) {
            Entry::Vacant(_) => return Err(TransactionError::TtsNotExists(tts).into()),
            Entry::Occupied(mut o) => {
                if *o.get() > 1 {
                    *o.get_mut() -= 1;
                    false
                } else {
                    o.remove();
                    true
                }
            }
        };
        Ok(())
    }

    /// Advance stored transaction id if the given `txn` is larger.
    pub fn may_advance_txn(&self, txn: u32) {
        may_advance_atomic!(self.txn, txn);
    }
}

pub type TransactionManagerRef = Arc<TransactionManager>;
