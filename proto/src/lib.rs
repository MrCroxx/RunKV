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
