# lake-query integration tests

Live Flight tests for the stateless query service.

## Invariants

- Use loopback services and isolated temporary storage.
- Query may proxy metadata-only write streams but never object payload bytes.
- Verify committed state through the shared registry, not proxy internals.

