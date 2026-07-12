# Bounded managed-object presigned read plan

**Goal:** delegate an immutable S3 object read without delegating storage
credentials or proxying bytes through Lake services.

## Task 1: Define the capability boundary

1. Add a redacted `PresignedRead` type with explicit URL access and expiry.
2. Extend `ManagedObjectStore` with a default typed unsupported method.
3. Validate TTL in one shared path before any store-specific signing.

## Task 2: Implement S3 signing

1. Reuse the existing strict S3 URI and managed-prefix parser.
2. Presign one `GetObject` request with the existing AWS client configuration.
3. Keep Range headers available to downstream HTTP clients and perform no GET.

## Task 3: Prove security and integration

1. Verify exact bucket/key/expiry signing with a no-network test client.
2. Reject foreign buckets, sibling prefixes, query fragments, and TTL bounds.
3. Prove Debug redaction and SDK delegation over an unreachable Query channel.
4. Prove local/default stores return a typed unsupported error.

## Task 4: Ship

1. Document capability handling and stable-identity separation.
2. Run spec lifecycle, strict clippy, full gate, independent review, and
   independent verification before merge.
