use crate::coding::BytesSerde;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Context {
    pub id: u64,
}

impl<'de> BytesSerde<'de> for Context {}
