use std::net::SocketAddr;

use http::header::CONTENT_TYPE;
use http::{Request, Response};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Error, Server};
use prometheus::{Encoder, TextEncoder};
use tracing::{error, info};

pub struct DefaultPrometheusExporter;

impl DefaultPrometheusExporter {
    pub fn init(addr: SocketAddr) {
        tokio::spawn(async move {
            info!("Prometheus service is set up on http://{}", addr);
            if let Err(e) = Server::bind(&addr)
                .serve(make_service_fn(|_| async move {
                    Ok::<_, Error>(service_fn(Self::serve))
                }))
                .await
            {
                error!("Prometheus service error: {}", e);
            }
        });
    }

    async fn serve(_request: Request<Body>) -> anyhow::Result<Response<Body>> {
        let encoder = TextEncoder::new();
        let mut buffer = Vec::with_capacity(4096);
        let metrics = prometheus::gather();
        encoder.encode(&metrics, &mut buffer).unwrap();
        let response = Response::builder()
            .status(200)
            .header(CONTENT_TYPE, encoder.format_type())
            .body(Body::from(buffer))?;
        Ok(response)
    }
}
