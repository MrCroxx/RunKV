use bytes::Bytes;

pub trait Partitioner: Send + Sync + 'static {
    /// Finish building current sstable if returns true.
    fn partition(&mut self, key: &[u8], value: Option<&[u8]>, timestamp: u64) -> bool;
}

pub type BoxedPartitioner = Box<dyn Partitioner>;

pub struct DefaultPartitioner {
    partition_points: Vec<Bytes>,
    offset: usize,
}

impl DefaultPartitioner {
    pub fn new(mut partition_points: Vec<Bytes>) -> Self {
        partition_points.sort();
        Self {
            partition_points,
            offset: 0,
        }
    }
}

impl Partitioner for DefaultPartitioner {
    fn partition(&mut self, key: &[u8], _value: Option<&[u8]>, _timestamp: u64) -> bool {
        if self.offset >= self.partition_points.len() {
            return false;
        }
        if key >= self.partition_points[self.offset] {
            self.offset += 1;
            return true;
        }
        false
    }
}

#[derive(Default)]
pub struct NoPartitioner;

impl Partitioner for NoPartitioner {
    fn partition(&mut self, _key: &[u8], _value: Option<&[u8]>, _timestamp: u64) -> bool {
        false
    }
}
