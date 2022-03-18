fn main() {
    tonic_build::configure()
        .compile(
            &[
                "src/proto/common.proto",
                "src/proto/manifest.proto",
                "src/proto/meta.proto",
                "src/proto/rudder.proto",
                "src/proto/wheel.proto",
            ],
            &["src/proto"],
        )
        .unwrap()
}
