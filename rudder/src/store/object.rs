use bytes::Bytes;
use runkv_storage::ObjectStoreRef;

use crate::{Error, MetaStore, Result};

// TODO: Remove allow dead code.
#[allow(dead_code)]
pub struct ObjectMetaStore {
    object_store: ObjectStoreRef,
    path: String,
}

// TODO: Remove allow dead code.
#[allow(dead_code)]
impl ObjectMetaStore {
    pub fn new(object_store: ObjectStoreRef, path: String) -> Self {
        Self { object_store, path }
    }

    async fn put(&self, key: &Bytes, value: Bytes) -> Result<()> {
        self.object_store
            .put(&self.key(key), value)
            .await
            .map_err(Error::StorageError)
    }

    async fn get(&self, key: &Bytes) -> Result<Bytes> {
        self.object_store
            .get(&self.key(key))
            .await
            .map_err(Error::StorageError)
    }

    fn key(&self, key: &Bytes) -> String {
        format!("{}/{}", self.path, base64::encode(key))
    }
}

impl MetaStore for ObjectMetaStore {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use runkv_storage::MemObjectStore;

    use super::*;

    #[tokio::test]
    async fn test_put_get() {
        let object_store = Arc::new(MemObjectStore::default());
        let store = ObjectMetaStore::new(object_store, "meta-test".to_string());
        let key = Bytes::from_static(b"test-key");
        let value = Bytes::from_static(b"test-value");
        store.put(&key, value.clone()).await.unwrap();
        let fetched_value = store.get(&key).await.unwrap();
        assert_eq!(fetched_value, value);
    }
}
