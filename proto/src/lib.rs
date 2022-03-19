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
