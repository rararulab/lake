// Copyright 2026 Rararulab
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Process-owned Prometheus recorder and private scrape endpoint.

use std::{
    env,
    ffi::OsStr,
    future::{Future, IntoFuture},
    net::SocketAddr,
    time::Duration,
};

use anyhow::{Context as _, anyhow, bail};
use axum::{
    Router,
    http::{Method, StatusCode, header},
    response::IntoResponse,
    routing::any,
};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

const METRICS_ADDR_ENV: &str = "LAKE_METRICS_ADDR";
const UPKEEP_INTERVAL: Duration = Duration::from_secs(30);

struct MetricsRuntime {
    listener: TcpListener,
    handle:   PrometheusHandle,
}

enum Stop<T> {
    Server(T),
    Shutdown,
    Metrics(anyhow::Result<()>),
}

pub(crate) async fn run_with_metrics<F, M, S>(
    service: &'static str,
    shutdown: F,
    make_server: M,
) -> anyhow::Result<()>
where
    F: Future<Output = ()>,
    M: FnOnce(CancellationToken) -> S,
    S: Future<Output = anyhow::Result<()>>,
{
    let cancellation = CancellationToken::new();
    let metrics = MetricsRuntime::start_from_env(service).await?;
    let server = make_server(cancellation.clone());
    run_owned(server, shutdown, metrics, cancellation).await
}

async fn run_owned<S, F>(
    server: S,
    shutdown: F,
    metrics: Option<MetricsRuntime>,
    cancellation: CancellationToken,
) -> anyhow::Result<()>
where
    S: Future<Output = anyhow::Result<()>>,
    F: Future<Output = ()>,
{
    let metrics_enabled = metrics.is_some();
    let metrics_shutdown = cancellation.clone();
    let metrics = async move {
        match metrics {
            Some(runtime) => runtime.run(metrics_shutdown).await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(server);
    tokio::pin!(shutdown);
    tokio::pin!(metrics);

    let stop = tokio::select! {
        result = &mut server => Stop::Server(result),
        () = &mut shutdown => Stop::Shutdown,
        result = &mut metrics => Stop::Metrics(result),
    };

    cancellation.cancel();
    match stop {
        Stop::Server(result) => {
            if metrics_enabled {
                (&mut metrics).await?;
            }
            result
        }
        Stop::Shutdown => {
            let result = (&mut server).await;
            if metrics_enabled {
                (&mut metrics).await?;
            }
            result
        }
        Stop::Metrics(result) => {
            let server_result = (&mut server).await;
            result.context("metrics runtime stopped before the server")?;
            server_result?;
            bail!("metrics runtime stopped unexpectedly")
        }
    }
}

impl MetricsRuntime {
    async fn start_from_env(service: &'static str) -> anyhow::Result<Option<Self>> {
        let Some(addr) = metrics_addr_from_env()? else {
            return Ok(None);
        };
        let listener = TcpListener::bind(addr)
            .await
            .with_context(|| format!("bind metrics listener {addr}"))?;
        let builder = PrometheusBuilder::new()
            .add_global_label("service", service)
            .add_global_label("version", env!("CARGO_PKG_VERSION"));
        let handle = builder
            .install_recorder()
            .context("install Prometheus recorder")?;
        describe_process_metrics();
        metrics::gauge!("lake_process_info").set(1.0);
        tracing::info!(%addr, service, "Prometheus metrics listener ready");
        Ok(Some(Self { listener, handle }))
    }

    async fn run(self, shutdown: CancellationToken) -> anyhow::Result<()> {
        run_metrics(self.listener, self.handle, shutdown).await
    }
}

async fn run_metrics(
    listener: TcpListener,
    handle: PrometheusHandle,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let scrape_handle = handle.clone();
    let app = Router::new().route(
        "/metrics",
        any(move |method: Method| {
            let handle = scrape_handle.clone();
            async move {
                if method != Method::GET {
                    return StatusCode::METHOD_NOT_ALLOWED.into_response();
                }
                (
                    [(
                        header::CONTENT_TYPE,
                        "text/plain; version=0.0.4; charset=utf-8",
                    )],
                    handle.render(),
                )
                    .into_response()
            }
        }),
    );
    let server_shutdown = shutdown.clone();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            server_shutdown.cancelled().await;
        })
        .into_future();
    tokio::pin!(server);
    let mut upkeep = tokio::time::interval(UPKEEP_INTERVAL);
    upkeep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    upkeep.tick().await;
    loop {
        tokio::select! {
            result = &mut server => return result.context("serve Prometheus metrics"),
            () = shutdown.cancelled() => return (&mut server).await.context("stop Prometheus metrics"),
            _ = upkeep.tick() => handle.run_upkeep(),
        }
    }
}

fn describe_process_metrics() {
    metrics::describe_gauge!("lake_process_info", "Static Lake process build information");
}

fn metrics_addr_from_env() -> anyhow::Result<Option<SocketAddr>> {
    match env::var_os(METRICS_ADDR_ENV) {
        Some(value) => parse_metrics_addr(&value).map(Some),
        None => Ok(None),
    }
}

fn parse_metrics_addr(value: &OsStr) -> anyhow::Result<SocketAddr> {
    let value = value
        .to_str()
        .ok_or_else(|| anyhow!("{METRICS_ADDR_ENV} is not valid UTF-8"))?;
    let addr: SocketAddr = value.parse().with_context(|| {
        format!("invalid {METRICS_ADDR_ENV} '{value}'; expected an IP socket address")
    })?;
    if !addr.ip().is_loopback() {
        bail!("{METRICS_ADDR_ENV} must be loopback; use a local collector or sidecar")
    }
    Ok(addr)
}

#[cfg(test)]
mod tests {
    use std::{
        net::{Ipv4Addr, SocketAddrV4},
        time::Duration,
    };

    use metrics_exporter_prometheus::PrometheusBuilder;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    async fn request(addr: SocketAddr, method: &str, path: &str) -> String {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(
                format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        String::from_utf8(response).unwrap()
    }

    #[tokio::test]
    async fn metrics_endpoint_is_loopback_only_and_owned_by_shutdown() {
        assert!(parse_metrics_addr(OsStr::new("0.0.0.0:9090")).is_err());
        assert!(parse_metrics_addr(OsStr::new("metrics.local:9090")).is_err());
        assert!(parse_metrics_addr(OsStr::new("127.0.0.1:9090")).is_ok());

        let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _recorder = metrics::set_default_local_recorder(&recorder);
        metrics::counter!("lake_test_scrapes_total").increment(3);
        let cancellation = CancellationToken::new();
        let runtime = MetricsRuntime { listener, handle };
        let task = tokio::spawn(run_owned(
            std::future::pending::<anyhow::Result<()>>(),
            std::future::pending::<()>(),
            Some(runtime),
            cancellation,
        ));

        let response = request(addr, "GET", "/metrics").await;
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("lake_test_scrapes_total 3"));
        assert!(
            request(addr, "HEAD", "/metrics")
                .await
                .starts_with("HTTP/1.1 405")
        );
        assert!(
            request(addr, "POST", "/metrics")
                .await
                .starts_with("HTTP/1.1 405")
        );
        assert!(
            request(addr, "GET", "/nope")
                .await
                .starts_with("HTTP/1.1 404")
        );

        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match TcpListener::bind(addr).await {
                    Ok(listener) => break listener,
                    Err(_) => tokio::task::yield_now().await,
                }
            }
        })
        .await
        .expect("outer future drop releases metrics listener");
    }
}
