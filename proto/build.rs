fn main() {
    tonic_build::configure()
        .compile(
            &[
                "src/proto/common.proto",
                "src/proto/manifest.proto",
                "src/proto/runkv.proto",
            ],
            &["src/proto"],
        )
        .unwrap()
}
