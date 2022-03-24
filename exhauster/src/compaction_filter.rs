use bytes::Bytes;

pub trait CompactionFilter {
    /// Keep the key value pair if `filter` returns true.
    fn filter(&mut self, key: &[u8], value: Option<&[u8]>, timestamp: u64) -> bool;
}

pub struct DefaultCompactionFilter {
    last_key: Bytes,
    watermark: u64,
    remove_tombstone: bool,
}

impl DefaultCompactionFilter {
    pub fn new(watermark: u64, remove_tombstone: bool) -> Self {
        Self {
            last_key: Bytes::default(),
            watermark,
            remove_tombstone,
        }
    }
}

impl CompactionFilter for DefaultCompactionFilter {
    #[allow(clippy::collapsible_else_if)]
    fn filter(&mut self, key: &[u8], value: Option<&[u8]>, timestamp: u64) -> bool {
        let mut retain = true;
        if key != self.last_key {
            if value.is_none() && self.remove_tombstone {
                retain = false;
            }
        } else {
            if timestamp < self.watermark {
                retain = false;
            }
        }
        self.last_key = Bytes::copy_from_slice(key);
        retain
    }
}

#[cfg(test)]
mod tests {

    use test_log::test;

    use super::*;

    #[test]
    fn test_default_compaction_filter() {
        #[allow(clippy::type_complexity)]
        let dataset: Vec<(&[u8], Option<&[u8]>, u64, bool)> = vec![
            (b"k1", Some(b"v1-20"), 20, true),
            (b"k1", Some(b"v1-10"), 10, true),
            (b"k1", Some(b"v1-1"), 1, false),
            (b"k2", None, 1, true),
            (b"k3", Some(b"v3-100"), 100, true),
            (b"k3", None, 15, true),
            (b"k3", None, 8, false),
            (b"k3", Some(b"v3-100"), 100, true),
            (b"k4", None, 100, true),
            (b"k4", Some(b"v4-20"), 20, true),
            (b"k4", Some(b"v4-8"), 8, false),
            (b"k4", None, 1, false),
        ];
        let mut filter = DefaultCompactionFilter::new(10, false);
        for data in dataset {
            assert_eq!(filter.filter(data.0, data.1, data.2), data.3)
        }
    }
}
