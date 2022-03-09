use bytes::{BufMut, Bytes, BytesMut};

pub fn full_key(user_key: &[u8], timestamp: u64) -> Bytes {
    let mut buf = BytesMut::from(user_key);
    buf.put_u64_le(!timestamp);
    buf.freeze()
}
