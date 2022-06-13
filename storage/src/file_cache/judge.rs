pub struct Stats {}

// FIXME: I don't want to but have to write a short article in the item names of enum [`Verb`], QwQ.
pub enum Verb {
    EvictFromBlockCache,
    RefillAfterCompaction,
}

pub trait Judge: Send + Sync + Clone + 'static {
    fn judge(&self, stats: &Stats, level: u64, sst: u64, block_idx: u32, len: u32) -> bool;
}

#[derive(Clone)]
pub struct DefaultJudge {}

impl Judge for DefaultJudge {
    fn judge(&self, _stats: &Stats, _level: u64, _sst: u64, _block_idx: u32, _len: u32) -> bool {
        todo!()
    }
}
