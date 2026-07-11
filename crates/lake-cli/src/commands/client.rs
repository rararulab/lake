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

//! `lake client` — a thin Flight control-plane client to a remote metasrv.
//!
//! Unlike the other subcommands, this one talks to a running metadata-layer
//! server over the network instead of wiring the tiers in-process. It is a
//! pure client: it holds no local storage, so it never builds a
//! [`Context`](super::Context) and needs only the server `--addr`. Each
//! subcommand issues the matching Flight `do_action` (`create_table`,
//! `drop_table`, `resolve`, `list_tables`/`list_namespaces`) against the
//! control plane defined in `lake-metasrv`.
//! Remote `drop_table` uses the server's durable tombstone protocol, so a
//! repeated request can finish cleanup after a crash or leader handoff.

use anyhow::Context as _;
use arrow_flight::{Action, Result as FlightResult, flight_service_client::FlightServiceClient};
use clap::Subcommand;
use futures::TryStreamExt;
use lake_flight::ClientSecurity;
use serde_json::json;
use tonic::{Code, Request, Status, transport::Channel};

use super::{security::metadata_client_security_from_env, table::parse_table_ref};

struct MetasrvClient {
    inner:    FlightServiceClient<Channel>,
    security: ClientSecurity,
}

/// Subcommands of `lake client`, each a single Flight `do_action` call.
#[derive(Subcommand)]
pub enum ClientCmd {
    /// Create and register a table on the remote metasrv.
    CreateTable {
        /// `<namespace>.<name>`, e.g. `robots.arm_left`.
        table:   String,
        /// Columns as `name:type` (types: i64, f64, utf8, bool, file).
        /// Repeatable.
        #[arg(long = "column", value_name = "name:type", required = true)]
        columns: Vec<String>,
    },
    /// Drop a table on the remote metasrv: delete its data and deregister it.
    DropTable {
        /// `<namespace>.<name>`, e.g. `robots.arm_left`.
        table: String,
    },
    /// Resolve a table to its registration, or report `not found`.
    Resolve {
        /// `<namespace>.<name>`, e.g. `robots.arm_left`.
        table: String,
    },
    /// List namespaces, or the tables in one namespace.
    List {
        /// Namespace to list tables for; omit to list namespaces.
        namespace: Option<String>,
    },
}

/// Dispatch a `lake client` subcommand against the metasrv at `addr`.
pub async fn run(addr: &str, cmd: ClientCmd) -> anyhow::Result<()> {
    let mut client = connect(addr).await?;
    match cmd {
        ClientCmd::CreateTable { table, columns } => {
            create_table(&mut client, &table, &columns).await
        }
        ClientCmd::DropTable { table } => drop_table(&mut client, &table).await,
        ClientCmd::Resolve { table } => resolve(&mut client, &table).await,
        ClientCmd::List { namespace } => list(&mut client, namespace.as_deref()).await,
    }
}

/// Open a Flight client with the configured metadata TLS and credential.
async fn connect(addr: &str) -> anyhow::Result<MetasrvClient> {
    let security = metadata_client_security_from_env()?;
    let endpoint = if addr.contains("://") {
        addr.to_owned()
    } else {
        security.endpoint_for_authority(addr)
    };
    let channel = security
        .connect(endpoint)
        .await
        .with_context(|| format!("cannot connect to metasrv at '{addr}'"))?;
    Ok(MetasrvClient {
        inner: FlightServiceClient::new(channel),
        security,
    })
}

/// Map a Flight [`Status`] to an actionable CLI error, calling out the
/// leadership-related codes writes can hit.
fn map_status(status: &Status) -> anyhow::Error {
    match status.code() {
        Code::Unavailable => anyhow::anyhow!(
            "metasrv unavailable (no leader elected yet, or the leader is unreachable): {}",
            status.message()
        ),
        Code::FailedPrecondition => anyhow::anyhow!(
            "metasrv rejected the request (leadership precondition failed): {}",
            status.message()
        ),
        _ => anyhow::anyhow!("metasrv error: {}", status.message()),
    }
}

/// Issue `action` and collect its (usually empty or one-shot) result stream.
async fn do_action(
    client: &mut MetasrvClient,
    action: Action,
) -> anyhow::Result<Vec<FlightResult>> {
    let request = client.security.authorize_request(Request::new(action));
    let response = client
        .inner
        .do_action(request)
        .await
        .map_err(|s| map_status(&s))?;
    response
        .into_inner()
        .try_collect::<Vec<_>>()
        .await
        .map_err(|s| map_status(&s))
}

/// Build an [`Action`] from a type tag and a JSON body value.
fn action(r#type: &str, body: &serde_json::Value) -> anyhow::Result<Action> {
    Ok(Action {
        r#type: r#type.to_owned(),
        body:   serde_json::to_vec(body)?.into(),
    })
}

async fn create_table(
    client: &mut MetasrvClient,
    table: &str,
    columns: &[String],
) -> anyhow::Result<()> {
    let table = parse_table_ref(table)?;
    let body = json!({
        "namespace": table.namespace.0,
        "name": table.name.0,
        "columns": columns,
    });
    do_action(client, action("create_table", &body)?).await?;
    println!("created {table}");
    Ok(())
}

async fn drop_table(client: &mut MetasrvClient, table: &str) -> anyhow::Result<()> {
    let table = parse_table_ref(table)?;
    let body = json!({ "namespace": table.namespace.0, "name": table.name.0 });
    do_action(client, action("drop_table", &body)?).await?;
    println!("dropped {table}");
    Ok(())
}

async fn resolve(client: &mut MetasrvClient, table: &str) -> anyhow::Result<()> {
    let table = parse_table_ref(table)?;
    let body = json!({ "namespace": table.namespace.0, "name": table.name.0 });
    let request = client
        .security
        .authorize_request(Request::new(action("resolve", &body)?));
    match client.inner.do_action(request).await {
        // `resolve` reports a missing table as a NotFound status; surface that
        // as ordinary output rather than an error.
        Err(status) if status.code() == Code::NotFound => {
            println!("{table}: not found");
            Ok(())
        }
        Err(status) => Err(map_status(&status)),
        Ok(response) => {
            let results = response
                .into_inner()
                .try_collect::<Vec<_>>()
                .await
                .map_err(|s| map_status(&s))?;
            let first = results.first().context("resolve returned no result")?;
            let reg: serde_json::Value = serde_json::from_slice(&first.body)?;
            println!("{table}:");
            println!("{}", serde_json::to_string_pretty(&reg)?);
            Ok(())
        }
    }
}

async fn list(client: &mut MetasrvClient, namespace: Option<&str>) -> anyhow::Result<()> {
    let action = match namespace {
        Some(ns) => action("list_tables", &json!({ "namespace": ns }))?,
        None => Action {
            r#type: "list_namespaces".to_owned(),
            body:   Vec::new().into(),
        },
    };
    let results = do_action(client, action).await?;
    let first = results.first().context("list returned no result")?;
    let names: Vec<String> = serde_json::from_slice(&first.body)?;
    for name in names {
        println!("{name}");
    }
    Ok(())
}
