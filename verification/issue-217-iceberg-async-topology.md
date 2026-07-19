# Issue #217 verification

## Delivered contract

- The source-controlled topology shows both interactive `DoGet` and durable
  `PollFlightInfo` Iceberg execution paths.
- Both paths preserve the same encrypted snapshot identity. A durable worker
  point-loads that identity and fails closed when upstream retention removed it.
- The topology and design text make the authority boundary explicit: durable
  state has neither external credentials nor Iceberg source-object bytes, and
  large external objects are read directly by Query.

## Visual and static checks

- Quick Look rendered `docs/assets/iceberg-federation.html` successfully.
- Manual visual inspection confirmed that component boundaries, both execution
  paths, labels, absent-path callout, and legend are legible at a 1600px render.
- The artifact remains standalone HTML/SVG with no runtime JavaScript or
  external image dependency.

## Verification

- `cargo check --workspace --locked`
- `prek run --all-files`
- `mise run gate`
- Independent scope/diff review: no P0 or P1 findings.
