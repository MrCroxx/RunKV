pub mod common {
    #![allow(clippy::all)]
    tonic::include_proto!("common");
}

pub mod manifest {
    #![allow(clippy::all)]
    tonic::include_proto!("manifest");
}

pub mod meta {
    #![allow(clippy::all)]
    tonic::include_proto!("meta");

    impl Eq for KeyRange {}

    impl PartialOrd for KeyRange {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(&other))
        }
    }

    impl Ord for KeyRange {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.start_key.cmp(&other.start_key)
        }
    }
}

pub mod rudder {
    #![allow(clippy::all)]
    tonic::include_proto!("rudder");
}

pub mod wheel {
    #![allow(clippy::all)]
    tonic::include_proto!("wheel");
}

pub mod exhauster {
    #![allow(clippy::all)]
    tonic::include_proto!("exhauster");
}

pub mod kv {
    #![allow(clippy::all)]
    tonic::include_proto!("kv");

    use runkv_common::coding::BytesSerde;

    impl<'de> BytesSerde<'de> for KvRequest {}
    impl<'de> BytesSerde<'de> for KvResponse {}

    impl KvRequest {
        pub fn r#type(&self) -> Type {
            let mut r#type = Type::TNone;
            for op in self.ops.iter() {
                match op.r#type() {
                    OpType::None => {}
                    OpType::Get => match r#type {
                        Type::TNone => r#type = Type::TGet,
                        Type::TGet | Type::TTxn => {}
                        _ => r#type = Type::TTxn,
                    },
                    OpType::Put => match r#type {
                        Type::TNone => r#type = Type::TPut,
                        Type::TPut | Type::TTxn => {}
                        _ => r#type = Type::TTxn,
                    },
                    OpType::Delete => match r#type {
                        Type::TNone => r#type = Type::TDelete,
                        Type::TDelete | Type::TTxn => {}
                        _ => r#type = Type::TTxn,
                    },
                    OpType::Snapshot => match r#type {
                        Type::TNone => r#type = Type::TSnapshot,
                        Type::TSnapshot | Type::TTxn => {}
                        _ => r#type = Type::TTxn,
                    },
                }
            }
            r#type
        }

        pub fn is_read_only(&self) -> bool {
            for op in self.ops.iter() {
                match op.r#type() {
                    OpType::None | OpType::Get | OpType::Snapshot => {}
                    OpType::Put | OpType::Delete => return false,
                }
            }
            true
        }
    }
}
