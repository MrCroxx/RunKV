use bytes::{Buf, BufMut};
use serde::Deserialize;

#[derive(Deserialize, Clone, Copy, Debug)]
pub enum CompressionAlgorithm {
    None,
    Lz4,
}

impl CompressionAlgorithm {
    pub fn encode(&self, buf: &mut impl BufMut) {
        let v = match self {
            Self::None => 0,
            Self::Lz4 => 1,
        };
        buf.put_u8(v);
    }

    pub fn decode(buf: &mut impl Buf) -> Result<Self, anyhow::Error> {
        match buf.get_u8() {
            0 => Ok(Self::None),
            1 => Ok(Self::Lz4),
            _ => Err(anyhow::anyhow!("not valid compression algorithm")),
        }
    }
}

impl From<CompressionAlgorithm> for u8 {
    fn from(ca: CompressionAlgorithm) -> Self {
        match ca {
            CompressionAlgorithm::None => 0,
            CompressionAlgorithm::Lz4 => 1,
        }
    }
}

impl From<CompressionAlgorithm> for u64 {
    fn from(ca: CompressionAlgorithm) -> Self {
        match ca {
            CompressionAlgorithm::None => 0,
            CompressionAlgorithm::Lz4 => 1,
        }
    }
}

impl TryFrom<u8> for CompressionAlgorithm {
    type Error = anyhow::Error;
    fn try_from(v: u8) -> core::result::Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::None),
            1 => Ok(Self::Lz4),
            _ => Err(anyhow::anyhow!("not valid compression algorithm")),
        }
    }
}
