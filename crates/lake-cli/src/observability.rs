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

use std::env;

use anyhow::{Context as _, anyhow, bail};
use tracing_subscriber::EnvFilter;

const DEFAULT_FILTER: &str = "lake=info,lake_query=info,lake_metasrv=info,lake_catalog=info";

enum LogFormat {
    Json,
    Pretty,
}

pub(crate) fn init_from_env() -> anyhow::Result<()> {
    let format = log_format_from_env()?;
    let filter = log_filter_from_env()?;
    let result = match format {
        LogFormat::Json => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(false)
            .json()
            .with_writer(std::io::stderr)
            .try_init(),
        LogFormat::Pretty => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(false)
            .pretty()
            .with_writer(std::io::stderr)
            .try_init(),
    };
    result.map_err(|error| anyhow!("initialize process logging: {error}"))
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
        sync::{Arc, Mutex},
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
}
