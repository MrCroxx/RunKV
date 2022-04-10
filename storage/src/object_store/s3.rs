use std::ops::Range;

use async_trait::async_trait;
use aws_sdk_s3::error::{GetObjectError, GetObjectErrorKind};
use aws_sdk_s3::types::SdkError;
use aws_sdk_s3::{Client, Endpoint, Region};
use aws_smithy_http::body::SdkBody;

use super::ObjectStore;
use crate::{ObjectStoreError, Result};

pub struct S3ObjectStore {
    client: Client,
    bucket: String,
}

impl S3ObjectStore {
    pub async fn new(bucket: String) -> Self {
        let config = aws_config::load_from_env().await;
        let client = Client::new(&config);
        Self { client, bucket }
    }

    /// Create a minio client. The server should be like `minio://key:secret@address:port/bucket`.
    pub async fn new_with_minio(server: &str) -> Self {
        let server = server.strip_prefix("minio://").unwrap();
        let (access_key_id, rest) = server.split_once(':').unwrap();
        let (secret_access_key, rest) = rest.split_once('@').unwrap();
        let (address, bucket) = rest.split_once('/').unwrap();

        let loader = aws_config::ConfigLoader::default();
        let builder = aws_sdk_s3::config::Builder::from(&loader.load().await);
        let builder = builder.region(Region::new("custom"));
        let builder = builder.endpoint_resolver(Endpoint::immutable(
            format!("http://{}", address).try_into().unwrap(),
        ));
        let builder = builder.credentials_provider(aws_sdk_s3::Credentials::from_keys(
            access_key_id,
            secret_access_key,
            None,
        ));
        let config = builder.build();
        let client = Client::from_conf(config);
        Self {
            client,
            bucket: bucket.to_string(),
        }
    }
}

fn err(err: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> ObjectStoreError {
    ObjectStoreError::S3(err.into().to_string())
}

#[async_trait]
impl ObjectStore for S3ObjectStore {
    async fn put(&self, path: &str, obj: Vec<u8>) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .body(SdkBody::from(obj).into())
            .key(path)
            .send()
            .await
            .map_err(err)?;
        Ok(())
    }

    async fn get(&self, path: &str) -> Result<Option<Vec<u8>>> {
        let req = self.client.get_object().bucket(&self.bucket).key(path);
        let rsp = match req.send().await {
            Ok(rsp) => rsp,
            Err(SdkError::ServiceError {
                err:
                    GetObjectError {
                        kind: GetObjectErrorKind::NoSuchKey(..),
                        ..
                    },
                ..
            }) => return Ok(None),
            Err(e) => return Err(err(e).into()),
        };
        let data = rsp.body.collect().await.map_err(err)?.into_bytes().to_vec();
        Ok(Some(data))
    }

    async fn get_range(&self, path: &str, range: Range<usize>) -> Result<Option<Vec<u8>>> {
        let req = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(path)
            .range(format!("bytes={}-{}", range.start, range.end - 1));
        let rsp = match req.send().await {
            Ok(rsp) => rsp,
            Err(SdkError::ServiceError {
                err:
                    GetObjectError {
                        kind: GetObjectErrorKind::NoSuchKey(..),
                        ..
                    },
                ..
            }) => return Ok(None),
            Err(e) => return Err(err(e).into()),
        };
        let data = rsp.body.collect().await.map_err(err)?.into_bytes().to_vec();
        Ok(Some(data))
    }

    async fn remove(&self, path: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await
            .map_err(err)?;
        Ok(())
    }
}
