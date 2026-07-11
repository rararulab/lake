# Tenant catalog and managed-object authorization plan

## Domain model

- `PrincipalId` and `TenantId` are value objects: identity is their validated
  canonical string, not pointer identity.
- `Principal` is an authenticated entity containing subject, tenant, finite
  namespace grants, and role (`User`, `QueryService`, `MetadataPeer`, `Admin`).
- `AuthorizationPolicy` is an immutable process-local snapshot. Request paths
  depend on the snapshot, never the metastore.
- Delegation is a capability of `QueryService`, not a string header any caller
  can opt into. Metasrv constructs an effective user context only after the
  authenticated peer role permits delegation.

Validation rejects empty/oversized identifiers, separators, `.`/`..`, control
characters, and values unsafe as a catalog name or storage path segment.

## Work sequence

1. Add validated identity/grant types and multi-token constant-time
   authentication in `lake-flight`, with redacted parsing/config errors.
2. Attach an explicit development principal in insecure loopback mode and
   preserve current exposure rules.
3. Add Query's request-local authorization gate before SQL planning and apply
   the same policy to Flight SQL discovery using only cached catalog state.
4. Carry trusted delegation over Query→Metasrv, independently enforce every
   metadata action/write, and preserve it through follower forwarding.
5. Derive tenant child managed-stage descriptors and prove SDK local/S3 prefix
   containment across two tenants.
6. Wire protected principal-map configuration through CLI processes; document
   IAM prefix requirements, rotation/restart behavior, and denial metrics.
7. Run spec lifecycle, TLS integration, LocalStack, strict Clippy, and the full
   gate; record evidence before review/merge.

## Safety notes

- SQL authorization must use parsed/planned table references, not substring or
  regex matching.
- A denial must be indistinguishable from a missing hidden resource.
- Policy reload is out of scope for the first slice: configuration is immutable
  for one process lifetime, making request behavior deterministic.
- The SDK's exact-prefix check is defense in depth. IAM remains the cloud data
  plane authority because Lake never distributes storage credentials.
