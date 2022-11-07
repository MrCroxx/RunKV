// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, BufMut};
use tokio::sync::{mpsc, Mutex};

use super::cache::FlushBufferHook;
use super::error::Result;
use crate::tiered_cache::{TieredCacheKey, TieredCacheValue};

#[derive(Clone, Hash, Debug, PartialEq, Eq)]
pub struct TestCacheKey(pub u64);

impl TieredCacheKey for TestCacheKey {
    fn encoded_len() -> usize {
        8
    }

    fn encode(&self, mut buf: &mut [u8]) {
        buf.put_u64(self.0);
    }

    fn decode(mut buf: &[u8]) -> Self {
        Self(buf.get_u64())
    }
}

pub type TestCacheValue = Vec<u8>;

impl TieredCacheValue for Vec<u8> {
    fn len(&self) -> usize {
        Vec::len(self)
    }

    fn encoded_len(&self) -> usize {
        self.len()
    }

    fn encode(&self, mut buf: &mut [u8]) {
        buf.put_slice(self)
    }

    fn decode(buf: Vec<u8>) -> Self {
        buf.to_vec()
    }
}

pub fn key(v: u64) -> TestCacheKey {
    TestCacheKey(v)
}

#[derive(Clone)]
pub struct FlushHolder {
    pre_sender: mpsc::UnboundedSender<()>,
    pre_receiver: Arc<Mutex<mpsc::UnboundedReceiver<()>>>,

    post_sender: mpsc::UnboundedSender<()>,
    post_receiver: Arc<Mutex<mpsc::UnboundedReceiver<()>>>,
}

impl Default for FlushHolder {
    fn default() -> Self {
        let (tx0, rx0) = mpsc::unbounded_channel();
        let (tx1, rx1) = mpsc::unbounded_channel();
        Self {
            pre_sender: tx0,
            pre_receiver: Arc::new(Mutex::new(rx0)),

            post_sender: tx1,
            post_receiver: Arc::new(Mutex::new(rx1)),
        }
    }
}

impl FlushHolder {
    pub fn trigger(&self) {
        self.pre_sender.send(()).unwrap();
    }

    pub async fn wait(&self) {
        self.post_receiver.lock().await.recv().await.unwrap();
    }
}

#[async_trait]
impl FlushBufferHook for FlushHolder {
    async fn pre_flush(&self) -> Result<()> {
        self.pre_receiver.lock().await.recv().await.unwrap();
        Ok(())
    }

    async fn post_flush(&self, _bytes: usize) -> Result<()> {
        self.post_sender.send(()).unwrap();
        Ok(())
    }
}

/// `datasize()` returns the actual data size of a file.
///
/// File systems like ext4 takes metadata blocks into account in `stat.st_blocks` of `fstat(2)`.
/// So it'not accurate if you really want to know the data size of sparse file with `fstat`.
///
/// `datasize` is implemented by iterates the `fiemap` of the file.
pub fn datasize(path: impl AsRef<Path>) -> Result<usize> {
    let mut size = 0;

    let fm = fiemap::fiemap(path)?;
    for fe in fm {
        let fe = fe.unwrap();
        size += fe.fe_length as usize;
    }

    Ok(size)
}
