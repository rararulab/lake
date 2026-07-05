# lake — North Star

## What lake is

lake is a lakehouse for embodied-AI data, in the spirit of LanceDB: robot
fleets write episode data (images, video, pointclouds, sensor streams) as
immutable files, and large batches of training/eval nodes read it back
concurrently through a SQL interface.

The bet: **immutable metadata plus a thin CAS pointer scales DDoS-like read
traffic without a hot central store.** The KV metastore holds only tiny
version pointers; manifests and data files are immutable and infinitely
cacheable on every reader node. DataFusion provides the SQL surface so
users query with plain SQL instead of a bespoke API.

## What lake is NOT

- **NOT a general-purpose data warehouse.** The workload is embodied-AI
  training/eval reads: huge scans, few point lookups, bursty fan-out.
  We do not optimize for BI dashboards or OLTP.
- **NOT a MySQL clone.** The SQL dialect is DataFusion's; the wire
  protocol direction is Arrow Flight SQL. We will not implement the MySQL
  wire protocol for compatibility's sake.
- **NOT a transaction engine.** One table, one version pointer, one CAS.
  No cross-table transactions, no MVCC beyond snapshot-by-version. Losers
  of a commit race retry.
- **NOT a storage format project.** We ride Parquet (and Lance when blob
  workloads demand it); we do not invent a file format.
- **NOT a metadata service.** If a design puts per-query load on the KV
  store proportional to reader count, it is wrong — readers must be
  servable from immutable, cached artifacts.

## What working lake looks like

- A fleet of N reader nodes issues the same SQL query simultaneously; the
  KV store sees O(1) traffic per version change, not O(N) per query.
- A writer commits a new table version while readers stream the old one;
  no reader ever observes a half-written snapshot.
- A new user points a client at the catalog and runs
  `SELECT ... FROM lake.public.<table>` with zero schema setup.
- `cargo run` proves the whole path end-to-end (ingest → commit → SQL) in
  one command, on a laptop, with RocksDB.
