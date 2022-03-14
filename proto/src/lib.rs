pub mod common {
    include!(concat!(env!("OUT_DIR"), "/common.rs"));
}

pub mod manifest {
    include!(concat!(env!("OUT_DIR"), "/manifest.rs"));
}

pub mod meta {
    include!(concat!(env!("OUT_DIR"), "/meta.rs"));
}

pub mod runkv {
    include!(concat!(env!("OUT_DIR"), "/runkv.rs"));
}
