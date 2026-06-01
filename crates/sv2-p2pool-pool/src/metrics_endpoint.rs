//! Tiny HTTP `/metrics` endpoint that scrapes a `prometheus::Registry`.
//!
//! Mounting any of `hyper` / `axum` / `warp` here would pull in a
//! large transitive dep tree just to render a single endpoint. The
//! Prometheus exposition format is plain text over HTTP/1.1; a manual
//! handler in ~50 lines does the job and stays isolated from the rest
//! of the pool's runtime.
//!
//! ## What it serves
//!
//! - `GET /metrics` → 200, `Content-Type: text/plain; version=0.0.4`,
//!   body is whatever `TextEncoder::encode_to_string(&registry.gather())`
//!   returns.
//! - Anything else → 404.
//!
//! ## What it does NOT do
//!
//! - HTTP/2, TLS, keep-alive, chunked encoding. Each request is
//!   handled to completion then the connection closes.
//! - Authentication. Operators should put it behind a private network
//!   or a reverse proxy.

use std::net::SocketAddr;

use prometheus::{Encoder, Registry, TextEncoder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// Errors emitted by the endpoint spawner.
#[derive(Debug, thiserror::Error)]
pub enum MetricsEndpointError {
    #[error("failed to bind {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
}

/// Spawn the `/metrics` server on `addr` against `registry`.
///
/// Returns the actual bound address (useful when the caller asked for
/// port 0) + the task `JoinHandle`. The server runs until the handle
/// is aborted; aborting is the only way to stop it.
pub async fn spawn_metrics_endpoint(
    addr: SocketAddr,
    registry: Registry,
) -> Result<(SocketAddr, JoinHandle<()>), MetricsEndpointError> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|source| MetricsEndpointError::Bind { addr, source })?;
    let bound = listener
        .local_addr()
        .map_err(|source| MetricsEndpointError::Bind { addr, source })?;
    info!(addr = %bound, "metrics endpoint listening on /metrics");

    let handle = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    let registry = registry.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_one(stream, &registry).await {
                            debug!(?peer, error = %e, "metrics handler exited with error");
                        }
                    });
                }
                Err(e) => {
                    warn!(error = %e, "metrics endpoint accept failed");
                }
            }
        }
    });

    Ok((bound, handle))
}

/// Read the request line, write the response, close.
async fn handle_one(mut stream: TcpStream, registry: &Registry) -> std::io::Result<()> {
    // Read until we see "\r\n\r\n" or the buffer fills up. Real-world
    // Prometheus scrapes are small; 4 KB is plenty for headers.
    let mut buf = [0u8; 4096];
    let mut len = 0;
    while len < buf.len() {
        let n = stream.read(&mut buf[len..]).await?;
        if n == 0 {
            break;
        }
        len += n;
        if buf[..len].windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }

    // Parse just the request-line; we only care about path.
    let request = std::str::from_utf8(&buf[..len]).unwrap_or("");
    let path = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");

    if path == "/metrics" {
        let metric_families = registry.gather();
        let mut body = Vec::new();
        let encoder = TextEncoder::new();
        if let Err(e) = encoder.encode(&metric_families, &mut body) {
            warn!(error = %e, "failed to encode metrics");
            write_status(&mut stream, 500, b"text/plain", b"encode error\n").await?;
            return Ok(());
        }
        let content_type = TextEncoder::new().format_type().to_string();
        write_status(&mut stream, 200, content_type.as_bytes(), &body).await?;
    } else {
        write_status(&mut stream, 404, b"text/plain", b"not found\n").await?;
    }

    Ok(())
}

async fn write_status(
    stream: &mut TcpStream,
    status: u16,
    content_type: &[u8],
    body: &[u8],
) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "Unknown",
    };
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: {ct}\r\n\
         Content-Length: {len}\r\n\
         Connection: close\r\n\
         \r\n",
        status = status,
        reason = reason,
        ct = std::str::from_utf8(content_type).unwrap_or("text/plain"),
        len = body.len(),
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.shutdown().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use prometheus::IntCounter;

    use super::*;

    async fn fetch(addr: SocketAddr, path: &str) -> (u16, String) {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n");
        stream.write_all(req.as_bytes()).await.expect("write");
        let mut buf = Vec::new();
        // Server closes the connection after writing; read_to_end
        // returns the full response.
        stream.read_to_end(&mut buf).await.expect("read");
        let response = String::from_utf8(buf).expect("utf-8");
        let (status_line, _) = response.split_once("\r\n").expect("split status");
        let status: u16 = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .expect("parse status");
        let body = response
            .split_once("\r\n\r\n")
            .map(|(_, b)| b)
            .unwrap_or("");
        (status, body.to_string())
    }

    #[tokio::test]
    async fn metrics_endpoint_serves_registered_counters() {
        let registry = Registry::new();
        let counter = IntCounter::new("test_counter", "test counter").unwrap();
        registry.register(Box::new(counter.clone())).unwrap();
        counter.inc();
        counter.inc();
        counter.inc();

        let (addr, handle) =
            spawn_metrics_endpoint(SocketAddr::from(([127, 0, 0, 1], 0)), registry)
                .await
                .expect("spawn");

        // /metrics returns the encoded counters.
        let (status, body) = tokio::time::timeout(Duration::from_secs(2), fetch(addr, "/metrics"))
            .await
            .expect("fetch within timeout");
        assert_eq!(status, 200);
        assert!(body.contains("test_counter 3"), "body: {body}");

        // Anything else returns 404.
        let (status_404, _) = tokio::time::timeout(Duration::from_secs(2), fetch(addr, "/nope"))
            .await
            .expect("fetch within timeout");
        assert_eq!(status_404, 404);

        handle.abort();
        let _ = handle.await;
    }
}
