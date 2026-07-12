# Restart-safe async query handles

Issue: #112

## Failure being closed

The server-side job and results survive replicas, but `query_async` currently
keeps its poll descriptor only in one future. A process crash loses the only
client capability. Retrying the original command also allocates a second job
if the first response was committed but lost.

## Protocol

The SDK puts a random 16-byte submission id in
`CommandStatementQuery.transaction_id`. Query does not support SQL
transactions, so this field is unambiguous on Lake's read-only PollFlightInfo
surface. Query derives a stable job key from authenticated tenant, principal,
and submission id, then CAS-creates the normal #110 record. Existing state is
returned only after its encrypted pinned statement matches exactly.

The returned SDK handle is caller-persistable but remains an opaque Flight
capability. Resuming sends its descriptor back to PollFlightInfo; it never
constructs a MetaStore or exposes state/object identities.

## Order

1. RED coordinator and Flight tests for retry convergence and statement alias
   rejection.
2. Deterministic identity-bound job ids and create-or-load CAS semantics.
3. Public bounded/redacted SDK handle plus explicit submit/resume/cancel/result
   APIs.
4. Restart/cross-replica/lost-response tests and convenience API delegation.
5. Documentation, lane-1 lifecycle, full gate, review, PR, merge.
