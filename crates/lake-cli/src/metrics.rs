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
use axum::{Router, http::header, routing::get};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;

const METRICS_ADDR_ENV: &str = "LAKE_METRICS_ADDR";
const UPKEEP_INTERVAL: Duration = Duration::from_secs(30);

struct MetricsRuntime {
    task: JoinHandle<anyhow::Result<()>>,
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
    let mut metrics = MetricsRuntime::start_from_env(service, cancellation.clone()).await?;
    let server = make_server(cancellation.clone());
    tokio::pin!(server);
    tokio::pin!(shutdown);

    let stop = if let Some(runtime) = metrics.as_mut() {
        tokio::select! {
            result = &mut server => Stop::Server(result),
            () = &mut shutdown => Stop::Shutdown,
            result = &mut runtime.task => Stop::Metrics(flatten_join(result)),
        }
    } else {
        tokio::select! {
            result = &mut server => Stop::Server(result),
            () = &mut shutdown => Stop::Shutdown,
        }
    };

    cancellation.cancel();
    match stop {
        Stop::Server(result) => {
            join_metrics(metrics).await?;
            result
        }
        Stop::Shutdown => {
            let result = (&mut server).await;
            join_metrics(metrics).await?;
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
    async fn start_from_env(
        service: &'static str,
        shutdown: CancellationToken,
    ) -> anyhow::Result<Option<Self>> {
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
        Ok(Some(Self {
            task: tokio::spawn(run_metrics(listener, handle, shutdown)),
        }))
    }
}

async fn join_metrics(metrics: Option<MetricsRuntime>) -> anyhow::Result<()> {
    if let Some(mut runtime) = metrics {
        flatten_join((&mut runtime.task).await)?;
    }
    Ok(())
}

fn flatten_join(result: Result<anyhow::Result<()>, tokio::task::JoinError>) -> anyhow::Result<()> {
    result.map_err(|error| anyhow!("metrics task failed: {error}"))?
}

async fn run_metrics(
    listener: TcpListener,
    handle: PrometheusHandle,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let scrape_handle = handle.clone();
    let app = Router::new().route(
        "/metrics",
        get(move || {
            let handle = scrape_handle.clone();
            async move {
                (
                    [(
                        header::CONTENT_TYPE,
                        "text/plain; version=0.0.4; charset=utf-8",
                    )],
                    handle.render(),
                )
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
    use std::net::{Ipv4Addr, SocketAddrV4};

    use metrics_exporter_prometheus::PrometheusBuilder;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

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
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run_metrics(listener, handle, shutdown.clone()));

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        let response = String::from_utf8(response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("lake_test_scrapes_total 3"));

        shutdown.cancel();
        task.await.unwrap().unwrap();
        TcpListener::bind(addr)
            .await
            .expect("metrics listener released");
    }
}
