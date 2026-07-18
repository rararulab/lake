# lake-iceberg

Read-only external Apache Iceberg federation for the Query tier. It connects
to one deployment-configured REST catalog, never Lake Metasrv or the Lake
registry.

## Invariants

- The external catalog owns Iceberg metadata, snapshots, commits, and GC.
- Expose only immutable static snapshot providers; no Iceberg write provider
  may reach DataFusion.
- Endpoint credentials, object bytes, and signed URLs never enter Lake
  metadata, logs, or statement tickets.
- Namespace and provider caches are bounded; no unbounded catalog listing is
  allowed on a Query request path.

## Layout

- `src/lib.rs` — validated connector configuration, snapshot resolution, and
  DataFusion provider adapter.
