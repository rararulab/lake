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

//! Process-owned structured logging configuration.

use std::{env, time::Duration};

use anyhow::{Context as _, anyhow, bail};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{SpanExporter, WithExportConfig as _};
use opentelemetry_sdk::{
    Resource,
    trace::{BatchConfigBuilder, BatchSpanProcessor, SdkTracerProvider},
};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt as _, util::SubscriberInitExt as _};
use url::Url;

const DEFAULT_FILTER: &str = "lake=info,lake_query=info,lake_metasrv=info,lake_catalog=info";

enum LogFormat {
    Json,
    Pretty,
}

const OTLP_MAX_QUEUE_SIZE: usize = 2_048;
const OTLP_MAX_EXPORT_BATCH_SIZE: usize = 256;
const DEFAULT_OTLP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_OTLP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
struct OtlpConfig {
    endpoint:              String,
    service_name:          String,
    export_timeout:        Duration,
    shutdown_timeout:      Duration,
    max_queue_size:        usize,
    max_export_batch_size: usize,
}

impl OtlpConfig {
    fn from_env() -> anyhow::Result<Option<Self>> {
        let mut error = None;
        let config = Self::from_lookup(|name| match env::var(name) {
            Ok(value) => Some(value),
            Err(env::VarError::NotPresent) => None,
            Err(env::VarError::NotUnicode(_)) => {
                error = Some(anyhow!("{name} is not valid UTF-8"));
                None
            }
        })?;
        match error {
            Some(error) => Err(error),
            None => Ok(config),
        }
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> anyhow::Result<Option<Self>> {
        let endpoint = lookup("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
            .or_else(|| lookup("OTEL_EXPORTER_OTLP_ENDPOINT"));
        let Some(endpoint) = endpoint else {
            return Ok(None);
        };
        let parsed = Url::parse(&endpoint).context("invalid OTLP collector endpoint")?;
        if !matches!(parsed.scheme(), "http" | "https")
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || !matches!(parsed.path(), "" | "/")
        {
            bail!(
                "invalid OTLP collector endpoint; expected an http(s) origin without credentials, \
                 query, fragment, or path"
            );
        }

        let service_name = lookup("OTEL_SERVICE_NAME").unwrap_or_else(|| "lake".to_owned());
        if service_name.is_empty()
            || service_name.len() > 64
            || !service_name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            bail!(
                "invalid OTEL_SERVICE_NAME; expected 1..=64 ASCII alphanumeric, '.', '_', or '-'"
            );
        }

        let shutdown_timeout = match lookup("LAKE_OTLP_SHUTDOWN_TIMEOUT_MS") {
            Some(value) => {
                let millis = value
                    .parse::<u64>()
                    .context("invalid LAKE_OTLP_SHUTDOWN_TIMEOUT_MS")?;
                let timeout = Duration::from_millis(millis);
                if timeout.is_zero() || timeout > MAX_OTLP_SHUTDOWN_TIMEOUT {
                    bail!("LAKE_OTLP_SHUTDOWN_TIMEOUT_MS must be in 1..=30000");
                }
                timeout
            }
            None => DEFAULT_OTLP_SHUTDOWN_TIMEOUT,
        };

        Ok(Some(Self {
            endpoint,
            service_name,
            export_timeout: shutdown_timeout / 2,
            shutdown_timeout,
            max_queue_size: OTLP_MAX_QUEUE_SIZE,
            max_export_batch_size: OTLP_MAX_EXPORT_BATCH_SIZE,
        }))
    }
}

pub(crate) struct TelemetryGuard {
    provider:         Option<SdkTracerProvider>,
    shutdown_timeout: Duration,
}

impl TelemetryGuard {
    fn disabled() -> Self {
        Self {
            provider:         None,
            shutdown_timeout: DEFAULT_OTLP_SHUTDOWN_TIMEOUT,
        }
    }

    fn from_config(config: OtlpConfig) -> anyhow::Result<Self> {
        let exporter = SpanExporter::builder()
            .with_tonic()
            .with_endpoint(config.endpoint.clone())
            // Leave the processor enough time to observe exporter completion,
            // shut the exporter down, and join its worker within the outer bound.
            .with_timeout(config.export_timeout)
            .build()
            .context("build OTLP trace exporter")?;
        Ok(Self::from_exporter(config, exporter))
    }

    fn from_exporter(
        config: OtlpConfig,
        exporter: impl opentelemetry_sdk::trace::SpanExporter + 'static,
    ) -> Self {
        let processor = BatchSpanProcessor::builder(exporter)
            .with_batch_config(
                BatchConfigBuilder::default()
                    .with_max_queue_size(config.max_queue_size)
                    .with_max_export_batch_size(config.max_export_batch_size)
                    .build(),
            )
            .build();
        let resource = Resource::builder()
            .with_service_name(config.service_name)
            .build();
        let provider = SdkTracerProvider::builder()
            .with_resource(resource)
            .with_span_processor(processor)
            .build();
        Self {
            provider:         Some(provider),
            shutdown_timeout: config.shutdown_timeout,
        }
    }

    pub(crate) fn shutdown(mut self) { self.shutdown_provider(); }

    fn shutdown_provider(&mut self) {
        let Some(provider) = self.provider.take() else {
            return;
        };
        if let Err(error) = provider.shutdown_with_timeout(self.shutdown_timeout) {
            tracing::warn!(%error, "OTLP trace exporter did not shut down cleanly");
        }
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) { self.shutdown_provider(); }
}

pub(crate) fn init_from_env() -> anyhow::Result<TelemetryGuard> {
    let format = log_format_from_env()?;
    let filter = log_filter_from_env()?;
    let guard = match OtlpConfig::from_env()? {
        Some(config) => TelemetryGuard::from_config(config)?,
        None => TelemetryGuard::disabled(),
    };
    let telemetry = guard
        .provider
        .as_ref()
        .map(|provider| tracing_opentelemetry::layer().with_tracer(provider.tracer("lake")));
    let subscriber = tracing_subscriber::registry().with(filter).with(telemetry);
    let result = match format {
        LogFormat::Json => subscriber
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .json()
                    .with_writer(std::io::stderr),
            )
            .try_init(),
        LogFormat::Pretty => subscriber
            .with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .pretty()
                    .with_writer(std::io::stderr),
            )
            .try_init(),
    };
    result
        .map(|()| guard)
        .map_err(|error| anyhow!("initialize process observability: {error}"))
}

fn log_format_from_env() -> anyhow::Result<LogFormat> {
    match env::var("LAKE_LOG_FORMAT") {
        Ok(value) if value == "json" => Ok(LogFormat::Json),
        Ok(value) if value == "pretty" => Ok(LogFormat::Pretty),
        Ok(value) => bail!("invalid LAKE_LOG_FORMAT '{value}'; expected 'json' or 'pretty'"),
        Err(env::VarError::NotPresent) => Ok(LogFormat::Json),
        Err(env::VarError::NotUnicode(_)) => bail!("LAKE_LOG_FORMAT is not valid UTF-8"),
    }
}

fn log_filter_from_env() -> anyhow::Result<EnvFilter> {
    match env::var("RUST_LOG") {
        Ok(value) => EnvFilter::try_new(value).context("invalid RUST_LOG filter"),
        Err(env::VarError::NotPresent) => {
            EnvFilter::try_new(DEFAULT_FILTER).context("invalid built-in log filter")
        }
        Err(env::VarError::NotUnicode(_)) => bail!("RUST_LOG is not valid UTF-8"),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::Write,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use opentelemetry::trace::{Span as _, Tracer as _, TracerProvider as _};
    use opentelemetry_sdk::{
        error::OTelSdkResult,
        trace::{SpanData, SpanExporter},
    };
    use tracing::Level;

    use super::*;

    #[derive(Clone, Default)]
    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("capture lock").write(bytes)
        }

        fn flush(&mut self) -> std::io::Result<()> { self.0.lock().expect("capture lock").flush() }
    }

    impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for SharedWriter {
        type Writer = Self;

        fn make_writer(&'writer self) -> Self::Writer { self.clone() }
    }

    #[derive(Clone, Debug, Default)]
    struct RecordingExporter {
        exported: Arc<AtomicUsize>,
        shutdown: Arc<AtomicBool>,
    }

    impl SpanExporter for RecordingExporter {
        async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
            self.exported.fetch_add(batch.len(), Ordering::Relaxed);
            Ok(())
        }

        fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
            self.shutdown.store(true, Ordering::Relaxed);
            Ok(())
        }
    }

    fn finish_test_span(guard: &TelemetryGuard) {
        let provider = guard.provider.as_ref().expect("enabled provider");
        let tracer = provider.tracer("lake-test");
        tracer.start("bounded-test-span").end();
    }

    #[test]
    fn default_filter_enables_lake_targets_only() {
        let writer = SharedWriter::default();
        let capture = writer.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::try_new(DEFAULT_FILTER).expect("default filter"))
            .with_ansi(false)
            .json()
            .with_writer(writer)
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            tracing::event!(target: "lake", Level::INFO, message = "lake event");
            tracing::event!(target: "lake_query", Level::INFO, message = "lake event");
            tracing::event!(target: "lake_metasrv", Level::INFO, message = "lake event");
            tracing::event!(target: "lake_catalog", Level::INFO, message = "lake event");
            tracing::event!(target: "hyper", Level::INFO, message = "dependency event");
        });

        let bytes = capture.0.lock().expect("capture lock").clone();
        let events = String::from_utf8(bytes).expect("JSON logs are UTF-8");
        let targets = events
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("JSON event"))
            .map(|event| event["target"].as_str().expect("event target").to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            targets,
            ["lake", "lake_query", "lake_metasrv", "lake_catalog"]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn otlp_exporter_is_opt_in_and_lifecycle_owned() {
        let disabled = OtlpConfig::from_lookup(|_| None).expect("disabled configuration");
        assert!(disabled.is_none());

        let malformed = OtlpConfig::from_lookup(|name| match name {
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" => Some("ftp://collector.invalid".to_owned()),
            _ => None,
        })
        .expect_err("non-HTTP collector endpoint rejected");
        assert!(malformed.to_string().contains("OTLP"));

        let config = OtlpConfig::from_lookup(|name| match name {
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" => Some("http://127.0.0.1:9".to_owned()),
            "OTEL_SERVICE_NAME" => Some("lake-query".to_owned()),
            "LAKE_OTLP_SHUTDOWN_TIMEOUT_MS" => Some("25".to_owned()),
            _ => None,
        })
        .expect("valid unavailable collector configuration")
        .expect("export enabled");
        assert_eq!(config.max_queue_size, 2_048);
        assert_eq!(config.max_export_batch_size, 256);

        let recorder = RecordingExporter::default();
        let guard = TelemetryGuard::from_exporter(config.clone(), recorder.clone());
        finish_test_span(&guard);
        guard.shutdown();
        assert_eq!(recorder.exported.load(Ordering::Relaxed), 1);
        assert!(recorder.shutdown.load(Ordering::Relaxed));

        let guard = TelemetryGuard::from_config(config).expect("provider construction is lazy");
        finish_test_span(&guard);
        let started = Instant::now();
        guard.shutdown();
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
