# Lake backport

Source: `datafusion-execution` 53.1.0 from crates.io (Apache-2.0).

Lake changes only `src/disk_manager.rs`: growth in
`RefCountedTempFile::update_disk_usage` is reserved with one atomic
compare/update before it is accepted. The crates.io implementation increments
the shared counter, returns an over-budget error, and leaves the rejected
increment behind because the file-local counter is never updated. That poisons
the process-wide `DiskManager` until restart.

The added unit test proves rejected quota is fully reusable. Lake's
`spill_budget_error_does_not_poison_runtime` additionally proves the behavior
through a real DataFusion external sort.

Remove this patch when Lance and Lake can move to a DataFusion release whose
spill writer reserves quota atomically. DataFusion main now performs this
rollback in its newer `FileSpillWriter` path; the 53/54 API line does not.
