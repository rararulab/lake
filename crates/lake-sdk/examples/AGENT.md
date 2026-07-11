# lake-sdk examples

Runnable, end-to-end SDK examples.

## Invariants

- Examples use only the public APIs they demonstrate.
- Each example must stream large-object bytes through `FileUpload`; never load
  the whole object into memory or send it through query/metadata services.
- Keep setup local and self-contained so `cargo run -p lake-sdk --example <name>`
  is sufficient to verify it.

