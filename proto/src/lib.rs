#![allow(clippy::all)]
pub mod common {
    tonic::include_proto!("common");
}

pub mod manifest {
    tonic::include_proto!("manifest");
}

pub mod meta {
    tonic::include_proto!("meta");
}

pub mod rudder {
    tonic::include_proto!("rudder");
}

pub mod wheel {
    tonic::include_proto!("wheel");
}
