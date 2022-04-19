use std::collections::BTreeMap;
use std::ops::Range;

use async_trait::async_trait;
use parking_lot::RwLock;

use super::ObjectStore;
use crate::{ObjectStoreError, Result};

#[derive(Default)]
pub struct MemObjectStore {
    objects: RwLock<BTreeMap<String, Vec<u8>>>,
}

#[async_trait]
impl ObjectStore for MemObjectStore {
    async fn put(&self, path: &str, obj: Vec<u8>) -> Result<()> {
        let mut objects = self.objects.write();
        objects.insert(path.to_string(), obj);
        Ok(())
    }

    async fn get(&self, path: &str) -> Result<Option<Vec<u8>>> {
        let objects = self.objects.read();
        let obj = objects.get(path).cloned();
        Ok(obj)
    }

    async fn get_range(&self, path: &str, range: Range<usize>) -> Result<Option<Vec<u8>>> {
        let objects = self.objects.read();
        let obj = objects.get(path).map(|obj| obj[range].to_vec());
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
