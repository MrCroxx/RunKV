fn main() {
    tonic_build::configure()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile(
            &[
                "src/proto/common.proto",
                "src/proto/manifest.proto",
                "src/proto/meta.proto",
                "src/proto/rudder.proto",
                "src/proto/wheel.proto",
                "src/proto/exhauster.proto",
            ],
            &["src/proto"],
        )
        .unwrap()
}
