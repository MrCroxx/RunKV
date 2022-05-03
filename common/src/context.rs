use crate::coding::BytesSerde;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Context {
    pub span_id: u64,
    pub request_id: u64,
}

impl<'de> BytesSerde<'de> for Context {}
