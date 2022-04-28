use isahc::config::Configurable;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

pub fn init_runkv_logger(service_name: &str) {
    let enable_jaeger_tracing = match std::env::var("RUNKV_TRACE") {
        Err(_) => false,
        Ok(val) => val.parse().unwrap(),
    };

    if enable_jaeger_tracing {
        opentelemetry::global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());

        let tracer = opentelemetry_jaeger::new_pipeline()
            // TODO: use UDP tracing in production environment
            .with_collector_endpoint("http://127.0.0.1:14268/api/traces")
            // TODO: change service name to compute-{port}
            .with_service_name(service_name)
            // disable proxy
            .with_http_client(isahc::HttpClient::builder().proxy(None).build().unwrap())
            .install_batch(opentelemetry::runtime::Tokio)
            .unwrap();

        // Configure RunKV's own crates to log at TRACE level, and ignore all third-party crates.
        let filter = tracing_subscriber::filter::Targets::new()
            // Enable trace for most modules.
            .with_target("runkv_rudder", tracing::Level::TRACE)
            .with_target("runkv_wheel", tracing::Level::TRACE)
            .with_target("runkv_exhauster", tracing::Level::TRACE)
            .with_target("runkv_storage", tracing::Level::TRACE)
            .with_target("runkv_tests", tracing::Level::TRACE)
            .with_target("openraft::raft", tracing::Level::TRACE)
            .with_target("raft", tracing::Level::TRACE)
            .with_target("events", tracing::Level::ERROR);

        let opentelemetry_layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(filter);

        tracing_subscriber::registry()
            .with(opentelemetry_layer)
            .init();
    }
}
