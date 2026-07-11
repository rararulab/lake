# Flight Transport Security Implementation Plan

**Goal:** Authenticate every Flight RPC and use verified TLS on all production
network hops without compromising Lake's stateless Query architecture.

## Architecture

Create `lake-flight` as the tonic-specific transport boundary. It exposes
opaque bearer credentials, server authentication/interception, server TLS
identity, client CA/domain configuration, and one secure Channel constructor.
Query, Metasrv, SDK, and CLI consume it; `lake-common` remains transport
neutral. Existing serve/connect helpers delegate to explicit loopback-insecure
configuration for compatibility, while non-loopback binding fails closed.

## Delivery order

1. Add failing `lake-flight` contract tests for credential validation,
   redaction, TLS client configuration, and exposure policy.
2. Implement minimal security primitives and strict errors.
3. Add failing Query authentication test, then secure its server and outbound
   Metasrv client.
4. Add failing Metasrv forwarding test, then secure follower-to-leader action
   and DoPut forwarding.
5. Add failing SDK full-chain TLS test, then introduce `LakeClientBuilder` and
   apply credentials to discovery, schema lookup, query, and append clients.
6. Wire CLI environment configuration, reject unsafe non-loopback deployment,
   and document certificate/token boundaries.
7. Run spec lifecycle, strict clippy, the full integration suite, and gate;
   rebase, re-run, open PR, and merge.

## Verification

```text
mise run spec-lifecycle specs/issue-14-flight-security.spec.md
cargo clippy -p lake-flight -p lake-query -p lake-metasrv -p lake-sdk -p lake-cli --all-targets -- -D warnings
mise run test-integration
mise run gate
```
