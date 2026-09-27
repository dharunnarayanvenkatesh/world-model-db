# Third-party license inventory

World Model DB uses only the linked dependency listed below. Reference clones
are pinned and inventoried separately: their code is studied, not compiled,
linked, copied, or shipped as part of the World Model DB binary.

| Project | Repository | Version / commit | License | Purpose | Usage |
|---|---|---|---|---|---|
| Rust toolchain and standard library | https://github.com/rust-lang/rust | 1.98.1, commit `48a229ceaefd4985c50990b14116b6d856af0985` | MIT OR Apache-2.0 | compiler, formatter, linter, standard library, and platform APIs | standard library linked by all crates; tools used for build and checks |
| redb | https://github.com/cberner/redb | 4.3.0, commit `2de02fa` | MIT OR Apache-2.0 | embedded transactional OLTP persistence | linked directly by `wm-storage`; no enabled transitive dependencies |
| Apache DataFusion | https://github.com/apache/datafusion | 55.1.0, commit `7d3835c` | Apache-2.0 | SQL/OLAP architecture reference | studied only; not linked because its resolved graph fails the strict license gate |

The DataFusion rejection details are recorded in
[`docs/license-audit-datafusion-55.1.0.md`](docs/license-audit-datafusion-55.1.0.md).
All other projects in the external reference map remain unlinked until a pinned
release and its complete transitive graph pass the same audit.
