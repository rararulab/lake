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
//! `drop_table`, `resolve`, `list_tables_page`/`list_namespaces_page`) against
//! the control plane defined in `lake-metasrv`.
//! Remote `drop_table` uses the server's durable tombstone protocol, so a
//! repeated request can finish cleanup after a crash or leader handoff.

use anyhow::Context as _;
use arrow_flight::{Action, Result as FlightResult, flight_service_client::FlightServiceClient};
use async_trait::async_trait;
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

/// The small control-plane surface needed by commands that follow an action
/// continuation. Keeping it separate from connection setup lets the page walk
/// be verified against real action bodies without starting a local metasrv.
#[async_trait]
trait ControlActionClient {
    async fn do_action(&mut self, action: Action) -> anyhow::Result<Vec<FlightResult>>;
}

#[async_trait]
impl ControlActionClient for MetasrvClient {
    async fn do_action(&mut self, action: Action) -> anyhow::Result<Vec<FlightResult>> {
        let request = self.security.authorize_request(Request::new(action));
        let response = self
            .inner
            .do_action(request)
            .await
            .map_err(|status| map_status(&status))?;
        response
            .into_inner()
            .try_collect::<Vec<_>>()
            .await
            .map_err(|status| map_status(&status))
    }
}

const CONTROL_ENUMERATION_PAGE_ENTRIES: usize = 128;

#[derive(serde::Deserialize, serde::Serialize)]
struct NamePage {
    names:        Vec<String>,
    continuation: Option<String>,
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
    client.do_action(action("create_table", &body)?).await?;
    println!("created {table}");
    Ok(())
}

async fn drop_table(client: &mut MetasrvClient, table: &str) -> anyhow::Result<()> {
    let table = parse_table_ref(table)?;
    let body = json!({ "namespace": table.namespace.0, "name": table.name.0 });
    client.do_action(action("drop_table", &body)?).await?;
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
    list_with_writer(client, namespace, |name| println!("{name}")).await
}

/// Follow remote control-plane name pages, handing each decoded name to
/// `write` before requesting the next page. This deliberately keeps no
/// complete catalog in the CLI process.
async fn list_with_writer<C, W>(
    client: &mut C,
    namespace: Option<&str>,
    mut write: W,
) -> anyhow::Result<()>
where
    C: ControlActionClient,
    W: FnMut(&str),
{
    let mut continuation = None;
    loop {
        let (action_type, body) = match namespace {
            Some(namespace) => (
                "list_tables_page",
                json!({
                    "namespace": namespace,
                    "limit": CONTROL_ENUMERATION_PAGE_ENTRIES,
                    "continuation": continuation.clone(),
                }),
            ),
            None => (
                "list_namespaces_page",
                json!({
                    "limit": CONTROL_ENUMERATION_PAGE_ENTRIES,
                    "continuation": continuation.clone(),
                }),
            ),
        };
        let results = client.do_action(action(action_type, &body)?).await?;
        let first = results.first().context("list page returned no result")?;
        let page: NamePage = serde_json::from_slice(&first.body)?;
        if results.len() != 1 {
            anyhow::bail!("list page returned more than one result");
        }
        for name in page.names {
            write(&name);
        }
        let Some(next) = page.continuation else {
            return Ok(());
        };
        continuation = Some(next);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use arrow_flight::{Action, Result as FlightResult};
    use async_trait::async_trait;
    use serde_json::json;

    use super::{ControlActionClient, NamePage, list_with_writer};

    struct ExpectedPage {
        names_written_before_request: Vec<String>,
        results: Vec<FlightResult>,
    }

    struct RecordingControlClient {
        pages:   VecDeque<ExpectedPage>,
        actions: Vec<Action>,
        written: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl ControlActionClient for RecordingControlClient {
        async fn do_action(&mut self, action: Action) -> anyhow::Result<Vec<FlightResult>> {
            let expected = self.pages.pop_front().expect("unexpected action request");
            assert_eq!(
                *self.written.lock().expect("written names lock"),
                expected.names_written_before_request,
                "the next page must not be requested before the prior page is written"
            );
            self.actions.push(action);
            Ok(expected.results)
        }
    }

    fn page_result(names: &[&str], continuation: Option<&str>) -> FlightResult {
        FlightResult {
            body: serde_json::to_vec(&NamePage {
                names:        names.iter().map(ToString::to_string).collect(),
                continuation: continuation.map(ToString::to_string),
            })
            .expect("serialize page")
            .into(),
        }
    }

    #[tokio::test]
    async fn client_list_follows_control_enumeration_pages() {
        let written = Arc::new(Mutex::new(Vec::new()));
        let mut client = RecordingControlClient {
            pages:   VecDeque::from([
                ExpectedPage {
                    names_written_before_request: Vec::new(),
                    results: vec![page_result(&["alpha"], Some("cursor-a"))],
                },
                ExpectedPage {
                    names_written_before_request: vec!["alpha".to_owned()],
                    results: vec![page_result(&["beta"], None)],
                },
            ]),
            actions: Vec::new(),
            written: written.clone(),
        };

        list_with_writer(&mut client, Some("robots"), {
            let written = written.clone();
            move |name| {
                written
                    .lock()
                    .expect("written names lock")
                    .push(name.to_owned())
            }
        })
        .await
        .expect("all action continuations succeed");

        assert!(client.pages.is_empty(), "every response page is consumed");
        assert_eq!(
            *written.lock().expect("written names lock"),
            ["alpha", "beta"],
            "names are written as each page is decoded"
        );
        assert_eq!(
            client
                .actions
                .iter()
                .map(|action| action.r#type.as_str())
                .collect::<Vec<_>>(),
            ["list_tables_page", "list_tables_page"],
            "remote CLI enumeration must not use legacy whole-catalog actions"
        );
        let bodies = client
            .actions
            .iter()
            .map(|action| serde_json::from_slice::<serde_json::Value>(&action.body).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            bodies,
            [
                json!({
                    "namespace": "robots",
                    "limit": super::CONTROL_ENUMERATION_PAGE_ENTRIES,
                    "continuation": null,
                }),
                json!({
                    "namespace": "robots",
                    "limit": super::CONTROL_ENUMERATION_PAGE_ENTRIES,
                    "continuation": "cursor-a",
                }),
            ],
            "each action carries the continuation returned by its prior page"
        );
    }
}
