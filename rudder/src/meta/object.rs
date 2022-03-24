use bytes::Bytes;
use runkv_storage::ObjectStoreRef;

use crate::error::{Error, Result};

pub struct ObjectMetaStore {
    object_store: ObjectStoreRef,
    path: String,
}

// TODO: Impl me.
// #[async_trait]
// impl MetaStore for ObjectMetaStore {}

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

    async fn get(&self, key: &Bytes) -> Result<Option<Bytes>> {
        self.object_store
            .get(&self.key(key))
            .await
            .map_err(Error::StorageError)
    }

    async fn remove(&self, key: &Bytes) -> Result<()> {
        self.object_store
            .remove(&self.key(key))
            .await
            .map_err(Error::StorageError)
    }

    fn key(&self, key: &Bytes) -> String {
        format!("{}/{}", self.path, base64::encode(key))
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use runkv_storage::MemObjectStore;
    use test_log::test;

    use super::*;

    #[test(tokio::test)]
    async fn test_crud() {
        let object_store = Arc::new(MemObjectStore::default());
        let store = ObjectMetaStore::new(object_store, "meta-test".to_string());
        let key = Bytes::from_static(b"test-key");
        let value = Bytes::from_static(b"test-value");
        store.put(&key, value.clone()).await.unwrap();
        let fetched_value = store.get(&key).await.unwrap().unwrap();
        assert_eq!(fetched_value, value);
        store.remove(&key).await.unwrap();
        assert!(store.get(&key).await.unwrap().is_none());
    }
}
