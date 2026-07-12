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

//! Explicit monotonic activation of catalog directory generations.

use clap::Args;
use serde::Serialize;

use super::Context;

#[derive(Args, Clone, Debug)]
pub struct CatalogFinalizeCmd {
    /// Confirm every registry writer publishes generation-capable mutations.
    #[arg(long, required = true)]
    pub acknowledge_writer_rollout: bool,

    /// Confirm registry write admission is quiescent during finalization.
    #[arg(long, required = true)]
    pub acknowledge_write_quiescence: bool,

    /// Emit one machine-readable JSON object.
    #[arg(long)]
    pub json: bool,
}

#[derive(Serialize)]
struct FinalizeOutput {
    authoritative: bool,
    finalized:     bool,
}

pub async fn run(ctx: &Context, command: CatalogFinalizeCmd) -> anyhow::Result<()> {
    let finalized = lake_meta::registry::finalize_directory_generation(ctx.meta.as_ref()).await?;
    let output = FinalizeOutput {
        authoritative: true,
        finalized,
    };
    if command.json {
        println!("{}", serde_json::to_string(&output)?);
    } else {
        println!("{}", serde_json::to_string_pretty(&output)?);
    }
    Ok(())
}
