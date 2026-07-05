# Code Comments

All comments and doc comments must be in English.

- New or modified `pub` items require `///` doc comments — but only comment what you touch, not unchanged code
- Crate/module docs use `//!` and should state purpose, invariants, and a useful example when one exists
- Inline comments explain **why**, not **what** — skip comments that restate the code
- Complex algorithms, non-obvious invariants, and safety-critical logic require comments even on private items
