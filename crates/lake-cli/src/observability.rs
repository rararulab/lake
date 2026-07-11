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
