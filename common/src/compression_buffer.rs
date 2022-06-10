use std::io::Write;

use bytes::BufMut;

use crate::coding::CompressionAlgorithm;

enum Encoder {
    None(Vec<u8>),
    Lz4(lz4::Encoder<Vec<u8>>),
}

pub struct CompressionBuffer {
    encoder: Encoder,
    raw_len: usize,
}

impl CompressionBuffer {
    pub fn new(algorithm: CompressionAlgorithm) -> Self {
        Self::with_capacity(algorithm, 4096)
    }

    pub fn with_capacity(algorithm: CompressionAlgorithm, capacity: usize) -> Self {
        let encoder = match algorithm {
            CompressionAlgorithm::None => Encoder::None(Vec::with_capacity(capacity)),
            CompressionAlgorithm::Lz4 => Encoder::Lz4(
                lz4::EncoderBuilder::new()
                    .level(4)
                    .build(Vec::with_capacity(capacity))
                    .unwrap(),
            ),
        };
        Self {
            encoder,
            raw_len: 0,
        }
    }

    pub fn algorithm(&self) -> CompressionAlgorithm {
        match &self.encoder {
            Encoder::None(_) => CompressionAlgorithm::None,
            Encoder::Lz4(_) => CompressionAlgorithm::Lz4,
        }
    }

    pub fn len(&self) -> usize {
        match &self.encoder {
            Encoder::None(buf) => buf.len(),
            Encoder::Lz4(encoder) => encoder.writer().len(),
        }
    }

    pub fn raw_len(&self) -> usize {
        self.raw_len
    }

    pub fn encode(&mut self, data: &[u8]) {
        match &mut self.encoder {
            Encoder::None(buf) => buf.put_slice(data),
            Encoder::Lz4(encoder) => encoder.write_all(data).unwrap(),
        }
        self.raw_len += data.len();
    }

    pub fn finish(self) -> Vec<u8> {
        let algorithm = self.algorithm();
        let mut buf = match self.encoder {
            Encoder::None(buf) => buf,
            Encoder::Lz4(encoder) => {
                let (buf, result) = encoder.finish();
                result.unwrap();
                buf
            }
        };
        algorithm.encode(&mut buf);
        buf
    }
}
