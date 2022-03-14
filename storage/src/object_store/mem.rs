use std::collections::BTreeMap;
use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;

use super::ObjectStore;
use crate::{ObjectStoreError, Result};

#[derive(Default)]
pub struct MemObjectStore {
    objects: RwLock<BTreeMap<String, Bytes>>,
}

#[async_trait]
impl ObjectStore for MemObjectStore {
    async fn put(&self, path: &str, obj: Bytes) -> Result<()> {
        let mut objects = self.objects.write();
        objects.insert(path.to_string(), obj);
        Ok(())
    }

    async fn get(&self, path: &str) -> Result<Option<Bytes>> {
        let objects = self.objects.read();
        let obj = objects.get(path).cloned();
        Ok(obj)
    }

    async fn get_range(&self, path: &str, range: Range<usize>) -> Result<Option<Bytes>> {
        let objects = self.objects.read();
        let obj = objects.get(path).map(|obj| obj.slice(range));
        Ok(obj)
    }

    async fn remove(&self, path: &str) -> Result<()> {
        let mut objects = self.objects.write();
        objects
            .remove(path)
            .ok_or_else(|| ObjectStoreError::ObjectNotFound(path.to_string()))?;
        Ok(())
    }
}
