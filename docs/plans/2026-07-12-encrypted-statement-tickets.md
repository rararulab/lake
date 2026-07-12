# Encrypted statement tickets implementation plan

## Goal

Replace raw-SQL Flight statement handles with stateless confidential,
authenticated, identity-bound, expiring capabilities that survive bounded key
rotation across Query replicas.

## Architecture

Keep the standard outer `TicketStatementQuery` so Flight SQL clients remain
compatible. Its statement handle contains a fixed binary header (magic,
version, 128-bit derived key id, random 64-bit process prefix, and monotonic
32-bit nonce suffix) and AES-256-GCM ciphertext. Header plus a fixed Query
audience is authenticated associated data. The encrypted protobuf payload
contains issue/expiry time, principal id, tenant id, and SQL. One active key
seals; up to three additional keys verify. Nonce-counter exhaustion fails
closed instead of risking reuse.

Production CLI loads a protected bounded JSON key ring shared by all replicas.
Loopback development generates one process-local key. DoGet performs
authenticated-principal extraction, ciphertext size validation, decryption,
identity/time checks, SQL size validation, authorization, then admission and
planning in that order.

## Tasks

1. RED/GREEN codec tests for confidentiality, tamper/time/audience/identity
   rejection, bounded rotation, and TTL changes.
2. RED/GREEN RPC tests proving cross-principal replay fails before planning.
3. Require shared keys for remote listeners and protected CLI file loading.
4. Wire Kubernetes secrets, TTL configuration, and staged rotation runbook.
5. Prove TLS multi-principal behavior, lane-1 lifecycle, strict Clippy, rustdoc,
   full gate, and independent security review.
