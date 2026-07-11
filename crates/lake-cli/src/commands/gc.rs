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

//! Separate managed-object GC worker.

use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context as _, ensure};
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Region;
use clap::Args;
use lake_common::{ManagedStageBackend, TableRef};
use lake_engine::{ObjectReferenceCursor, ObjectReferenceRequest};
use lake_meta::registry::{self, TableRegistration};
use lake_objects::{
    DeleteOutcome, GcApplyProgress, GcPlan, GcPlanApplier, GcPlanWriter, GcPlanner, InventoryPage,
    InventoryRequest, LiveReferenceIndex, LiveReferenceIndexBuilder, LocalObjectStore,
    ManagedObjectDeleter, ManagedObjectInventory, ObjectCandidate, ObjectError, S3ObjectStore,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::Context;

#[derive(Clone, Debug, Args)]
pub struct GcCmd {
    /// Immutable plan directory to create (dry-run) or consume (`--apply`).
    #[arg(long)]
    pub plan:            PathBuf,
    /// Minimum object age before it may enter a dry-run plan.
    #[arg(
        long,
        value_parser = clap::value_parser!(u64).range(1..),
        required_unless_present = "apply"
    )]
    pub safety_age_secs: Option<u64>,
    /// Mutate storage by consuming the exact immutable plan.
    #[arg(long)]
    pub apply:           bool,
    /// Durable apply progress; required with `--apply`.
    #[arg(long, required_if_eq("apply", "true"))]
    pub checkpoint:      Option<PathBuf>,
    /// Emit machine-readable JSON summary output.
    #[arg(long)]
    pub json:            bool,
}

pub async fn run(ctx: &Context, command: GcCmd) -> anyhow::Result<()> {
    let store = GcStore::open(ctx.managed_stage().backend()).await?;
    if command.apply {
        apply(ctx, &store, command).await
    } else {
        dry_run(ctx, &store, command).await
    }
}

async fn dry_run(ctx: &Context, store: &GcStore, command: GcCmd) -> anyhow::Result<()> {
    let safety_age_secs = command
        .safety_age_secs
        .context("--safety-age-secs is required for dry-run planning")?;
    let now_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before Unix epoch")?
            .as_millis(),
    )
    .context("current time exceeds u64 milliseconds")?;
    let safety_ms = safety_age_secs
        .checked_mul(1_000)
        .context("safety age overflows milliseconds")?;
    let cutoff_ms = now_ms
        .checked_sub(safety_ms)
        .context("safety age is greater than time since Unix epoch")?;
    let work_parent = command.plan.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(work_parent)
        .with_context(|| format!("create GC plan parent {}", work_parent.display()))?;
    let work_dir = work_parent.join(format!(".lake-gc-work-{}", Uuid::now_v7()));
    std::fs::create_dir(&work_dir)
        .with_context(|| format!("create GC work directory {}", work_dir.display()))?;

    let result = dry_run_in(ctx, store, &command.plan, &work_dir, cutoff_ms).await;
    let cleanup = std::fs::remove_dir_all(&work_dir);
    if let Err(error) = cleanup {
        if result.is_ok() {
            return Err(error).context("remove completed GC work directory");
        }
    }
    let plan = result?;
    print_plan(&plan, command.json);
    Ok(())
}

async fn dry_run_in(
    ctx: &Context,
    store: &GcStore,
    plan_path: &Path,
    work_dir: &Path,
    cutoff_ms: u64,
) -> anyhow::Result<GcPlan> {
    let roots = registry_snapshot(ctx).await?;
    let root_fingerprint = registry_fingerprint(&roots)?;
    let references = build_reference_index(ctx, &roots, work_dir).await?;
    let inventory_path = work_dir.join("inventory.jsonl");
    spool_inventory(store, &inventory_path).await?;

    // A moving registry root invalidates the whole mark phase. This catches
    // concurrent table creates, drops, appends, and maintenance commits before
    // any immutable plan is published.
    ensure!(
        registry_snapshot(ctx).await? == roots,
        "registry changed during GC planning; retry from a fresh snapshot"
    );

    let prefix = ManagedObjectInventory::managed_uri_prefix(store);
    let planner = GcPlanner::try_new(&prefix, cutoff_ms, 256, true)?;
    let inventory = CandidateIter::open(&inventory_path)?;
    let live = references.open()?;
    let pages = planner.plan_fallible(inventory, live);
    GcPlanWriter::try_new(plan_path, prefix, cutoff_ms, 256)?
        .with_source_fingerprint(root_fingerprint)
        .write(pages)
        .map_err(Into::into)
}

async fn build_reference_index(
    ctx: &Context,
    roots: &BTreeMap<TableRef, TableRegistration>,
    work_dir: &Path,
) -> anyhow::Result<LiveReferenceIndex> {
    // The engine page calls are async, while spill writes are synchronous and
    // bounded. Run them in the caller's async phase through the stateful build
    // below; no table's full lineage is retained.
    let builder = LiveReferenceIndexBuilder::try_new(work_dir, 65_536, 32)?;
    let build = builder.begin()?;
    fill_reference_index(ctx, roots, build).await
}

async fn fill_reference_index(
    ctx: &Context,
    roots: &BTreeMap<TableRef, TableRegistration>,
    mut build: lake_objects::LiveReferenceIndexBuild,
) -> anyhow::Result<LiveReferenceIndex> {
    for (table, registration) in roots {
        ensure!(
            registration.engine == ctx.engine.kind(),
            "table {table} uses unsupported engine '{}'",
            registration.engine
        );
        let mut cursor: Option<ObjectReferenceCursor> = None;
        loop {
            let request = ObjectReferenceRequest::try_new(
                registration.current_version,
                cursor.clone(),
                1_024,
            )?;
            let page = ctx
                .engine
                .retained_object_references(&registration.location, request)
                .await
                .with_context(|| format!("read retained object lineage for {table}"))?;
            for delta in page.deltas() {
                build.push_delta(delta.clone())?;
            }
            let next = page.next_cursor().cloned();
            let Some(next) = next else {
                break;
            };
            ensure!(
                Some(next.as_str()) != cursor.as_ref().map(ObjectReferenceCursor::as_str),
                "engine repeated the object-reference cursor for {table}"
            );
            cursor = Some(next);
        }
    }
    build.finish().map_err(Into::into)
}

async fn registry_snapshot(ctx: &Context) -> anyhow::Result<BTreeMap<TableRef, TableRegistration>> {
    let mut snapshot = BTreeMap::new();
    for namespace in registry::list_namespaces(ctx.meta.as_ref()).await? {
        for name in registry::list(ctx.meta.as_ref(), &namespace).await? {
            let table = TableRef {
                namespace: namespace.clone(),
                name,
            };
            if let Some(registration) = registry::get(ctx.meta.as_ref(), &table).await? {
                snapshot.insert(table, registration);
            }
        }
    }
    Ok(snapshot)
}

fn registry_fingerprint(roots: &BTreeMap<TableRef, TableRegistration>) -> anyhow::Result<String> {
    let ordered = roots.iter().collect::<Vec<_>>();
    let bytes = serde_json::to_vec(&ordered).context("encode GC registry roots")?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

async fn spool_inventory(store: &GcStore, path: &Path) -> anyhow::Result<()> {
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    let mut cursor = None;
    loop {
        let page = store
            .inventory_page(InventoryRequest::try_new(cursor.clone(), 1_000)?)
            .await?;
        write_inventory_page(&mut writer, &page)?;
        let next = page.next_cursor().map(ToOwned::to_owned);
        let Some(next) = next else {
            break;
        };
        ensure!(
            Some(&next) != cursor.as_ref(),
            "object inventory repeated its cursor"
        );
        cursor = Some(next);
    }
    writer.flush().context("flush object inventory spool")?;
    writer
        .get_ref()
        .sync_all()
        .context("sync object inventory spool")
}

fn write_inventory_page(writer: &mut BufWriter<File>, page: &InventoryPage) -> anyhow::Result<()> {
    for candidate in page.candidates() {
        serde_json::to_writer(&mut *writer, candidate).context("encode object inventory entry")?;
        writer
            .write_all(b"\n")
            .context("write object inventory entry")?;
    }
    Ok(())
}

async fn apply(ctx: &Context, store: &GcStore, command: GcCmd) -> anyhow::Result<()> {
    let checkpoint = command
        .checkpoint
        .context("--checkpoint is required with --apply")?;
    let plan = GcPlan::open(&command.plan)?;
    let planned_roots = plan
        .source_fingerprint()
        .context("GC plan has no registry-root fingerprint")?
        .to_owned();
    ensure!(
        registry_fingerprint(&registry_snapshot(ctx).await?)? == planned_roots,
        "registry no longer matches the GC plan; create a fresh dry-run plan"
    );
    let mut applier = GcPlanApplier::open(&command.plan, checkpoint).await?;
    let progress = loop {
        ensure!(
            registry_fingerprint(&registry_snapshot(ctx).await?)? == planned_roots,
            "registry changed during GC apply; create a fresh dry-run plan"
        );
        let progress = applier.apply_next(store).await?;
        if progress.is_complete() {
            break progress;
        }
    };
    print_progress(&progress, command.json);
    Ok(())
}

fn print_plan(plan: &GcPlan, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::json!({
                "mode": "dry-run",
                "plan_digest": plan.digest(),
                "pages": plan.page_count(),
                "candidates": plan.candidate_count(),
                "bytes": plan.total_size_bytes(),
            })
        );
    } else {
        println!(
            "dry-run plan={} pages={} candidates={} bytes={}",
            plan.digest(),
            plan.page_count(),
            plan.candidate_count(),
            plan.total_size_bytes()
        );
    }
}

fn print_progress(progress: &GcApplyProgress, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::json!({
                "mode": "apply",
                "complete": progress.is_complete(),
                "completed_pages": progress.completed_pages(),
                "processed_objects": progress.processed_objects(),
                "deleted_objects": progress.deleted_objects(),
                "absent_objects": progress.absent_objects(),
            })
        );
    } else {
        println!(
            "apply complete={} pages={} processed={} deleted={} absent={}",
            progress.is_complete(),
            progress.completed_pages(),
            progress.processed_objects(),
            progress.deleted_objects(),
            progress.absent_objects()
        );
    }
}

enum GcStore {
    Local(LocalObjectStore),
    S3(S3ObjectStore),
}

impl GcStore {
    async fn open(backend: &ManagedStageBackend) -> anyhow::Result<Self> {
        match backend {
            ManagedStageBackend::Local { root } => {
                Ok(Self::Local(LocalObjectStore::open(root).await?))
            }
            ManagedStageBackend::S3 {
                bucket,
                prefix,
                region,
                endpoint,
                force_path_style,
            } => {
                let mut loader = aws_config::defaults(BehaviorVersion::latest());
                if let Some(region) = region {
                    loader = loader.region(Region::new(region.clone()));
                }
                let shared = loader.load().await;
                let mut config =
                    aws_sdk_s3::config::Builder::from(&shared).force_path_style(*force_path_style);
                if let Some(endpoint) = endpoint {
                    config = config.endpoint_url(endpoint);
                }
                Ok(Self::S3(S3ObjectStore::new(
                    aws_sdk_s3::Client::from_conf(config.build()),
                    bucket,
                    prefix,
                )?))
            }
        }
    }
}

#[async_trait]
impl ManagedObjectInventory for GcStore {
    fn managed_uri_prefix(&self) -> String {
        match self {
            Self::Local(store) => ManagedObjectInventory::managed_uri_prefix(store),
            Self::S3(store) => ManagedObjectInventory::managed_uri_prefix(store),
        }
    }

    async fn inventory_page(
        &self,
        request: InventoryRequest,
    ) -> lake_objects::Result<InventoryPage> {
        match self {
            Self::Local(store) => store.inventory_page(request).await,
            Self::S3(store) => store.inventory_page(request).await,
        }
    }
}

#[async_trait]
impl ManagedObjectDeleter for GcStore {
    fn managed_uri_prefix(&self) -> String {
        match self {
            Self::Local(store) => ManagedObjectDeleter::managed_uri_prefix(store),
            Self::S3(store) => ManagedObjectDeleter::managed_uri_prefix(store),
        }
    }

    async fn delete_candidate(
        &self,
        candidate: &ObjectCandidate,
    ) -> lake_objects::Result<DeleteOutcome> {
        match self {
            Self::Local(store) => store.delete_candidate(candidate).await,
            Self::S3(store) => store.delete_candidate(candidate).await,
        }
    }
}

struct CandidateIter {
    path:  PathBuf,
    lines: std::io::Lines<BufReader<File>>,
}

impl CandidateIter {
    fn open(path: &Path) -> lake_objects::Result<Self> {
        let file = File::open(path).map_err(|source| ObjectError::GcPlanIo {
            action: "opening inventory spool",
            path: path.to_path_buf(),
            source,
        })?;
        Ok(Self {
            path:  path.to_path_buf(),
            lines: BufReader::new(file).lines(),
        })
    }
}

impl Iterator for CandidateIter {
    type Item = lake_objects::Result<ObjectCandidate>;

    fn next(&mut self) -> Option<Self::Item> {
        let line = match self.lines.next()? {
            Ok(line) => line,
            Err(source) => {
                return Some(Err(ObjectError::GcPlanIo {
                    action: "reading inventory spool",
                    path: self.path.clone(),
                    source,
                }));
            }
        };
        Some(
            serde_json::from_str(&line).map_err(|source| ObjectError::GcPlanCorrupt {
                path: self.path.clone(),
                source,
            }),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::FileTimes, time::Duration};

    use super::*;

    #[tokio::test]
    async fn local_gc_dry_run_then_apply_uses_no_server() {
        let temp = tempfile::tempdir().unwrap();
        let data_dir = temp.path().join("data");
        let ctx = Context::open(data_dir.to_str().unwrap()).await.unwrap();
        let managed = data_dir.join("managed-objects");
        std::fs::create_dir_all(&managed).unwrap();
        let orphan = managed.join("old-orphan");
        std::fs::write(&orphan, b"orphan").unwrap();
        std::fs::File::options()
            .write(true)
            .open(&orphan)
            .unwrap()
            .set_times(FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_secs(1)))
            .unwrap();
        let plan = temp.path().join("plan");

        run(
            &ctx,
            GcCmd {
                plan:            plan.clone(),
                safety_age_secs: Some(1),
                apply:           false,
                checkpoint:      None,
                json:            true,
            },
        )
        .await
        .unwrap();
        assert_eq!(GcPlan::open(&plan).unwrap().candidate_count(), 1);
        assert!(orphan.exists(), "dry-run must not mutate storage");

        run(
            &ctx,
            GcCmd {
                plan,
                safety_age_secs: None,
                apply: true,
                checkpoint: Some(temp.path().join("apply.json")),
                json: true,
            },
        )
        .await
        .unwrap();
        assert!(!orphan.exists());
    }
}
