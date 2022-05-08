use isahc::config::Configurable;
use tracing_subscriber::filter::{EnvFilter, Targets};
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

pub struct LogGuard {
    _file_appender_guard: tracing_appender::non_blocking::WorkerGuard,
    jaeger_enabled: bool,
}

impl Drop for LogGuard {
    fn drop(&mut self) {
        if self.jaeger_enabled {
            opentelemetry::global::shutdown_tracer_provider();
        }
    }
}

pub fn init_runkv_logger(service: &str, id: u64, log_path: &str) -> LogGuard {
    let jaeger_enabled = match std::env::var("RUNKV_TRACE") {
        Err(_) => false,
        Ok(val) => val.parse().unwrap(),
    };

    let (file_appender, file_appender_guard) = tracing_appender::non_blocking(
        tracing_appender::rolling::daily(log_path, format!("runkv-{}-{}.log", service, id)),
    );

    let guard = LogGuard {
        _file_appender_guard: file_appender_guard,
        jaeger_enabled,
    };

    let logger = tracing_subscriber::registry().with(EnvFilter::from_default_env());

    let fmt_layer = {
        // Configure RunKV's own crates to log at TRACE level, and ignore all third-party crates.
        let filter = Targets::new()
            // Enable trace for most modules.
            .with_target("runkv_common", tracing::Level::TRACE)
            .with_target("runkv_storage", tracing::Level::TRACE)
            .with_target("runkv_rudder", tracing::Level::TRACE)
            .with_target("runkv_wheel", tracing::Level::TRACE)
            .with_target("runkv_exhauster", tracing::Level::TRACE)
            .with_target("runkv_tests", tracing::Level::TRACE)
            .with_target("openraft::raft", tracing::Level::TRACE)
            .with_target("raft", tracing::Level::TRACE)
            .with_target("events", tracing::Level::WARN);

        tracing_subscriber::fmt::layer()
            .with_test_writer()
            .with_writer(file_appender)
            .with_ansi(false)
            .with_filter(filter)
    };

    let logger = logger.with(fmt_layer);

    if jaeger_enabled {
        opentelemetry::global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());

        // Configure RunKV's own crates to log at TRACE level, and ignore all third-party crates.
        let filter = Targets::new()
            // Enable trace for most modules.
            .with_target("runkv_common", tracing::Level::TRACE)
            .with_target("runkv_storage", tracing::Level::TRACE)
            .with_target("runkv_rudder", tracing::Level::TRACE)
            .with_target("runkv_wheel", tracing::Level::TRACE)
            .with_target("runkv_exhauster", tracing::Level::TRACE)
            .with_target("runkv_tests", tracing::Level::TRACE)
            .with_target("openraft::raft", tracing::Level::TRACE)
            .with_target("raft", tracing::Level::TRACE)
            .with_target("events", tracing::Level::WARN);

        let tracer = opentelemetry_jaeger::new_pipeline()
            // TODO: use UDP tracing in production environment
            .with_collector_endpoint("http://127.0.0.1:14268/api/traces")
            // TODO: change service name to compute-{port}
            .with_service_name(service)
            // disable proxy
            .with_http_client(isahc::HttpClient::builder().proxy(None).build().unwrap())
            .install_batch(opentelemetry::runtime::Tokio)
            .unwrap();

        let opentelemetry_layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(filter);

        logger.with(opentelemetry_layer).init();
    } else {
        logger.init();
    }

    guard
}
