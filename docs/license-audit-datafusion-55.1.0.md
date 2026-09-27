# DataFusion 55.1.0 dependency audit

Status: **reference clone permitted; linked integration rejected**.

Apache DataFusion 55.1.0 is Apache-2.0 and is pinned at commit
`7d3835c71f30cbd3c3ae4041732267f1f453097a`. Its minimal `sql` feature graph
was resolved with Cargo on 2026-09-27. The graph contains dependencies whose
license expressions cannot be satisfied solely by selecting MIT or
Apache-2.0:

| Crate | Resolved version | License expression | Result |
|---|---:|---|---|
| `foldhash` | 0.2.0 | Zlib | rejected |
| `zstd-safe` | 7.3.0 | BSD-3-Clause | rejected |
| `zstd-sys` | 2.1.0+zstd.1.5.7 | BSD-3-Clause | rejected |
| `unicode-ident` | 1.0.26 | `(MIT OR Apache-2.0) AND Unicode-3.0` | rejected |

Dual-license expressions containing an allowed choice, such as
`Apache-2.0 OR BSL-1.0`, can be consumed under Apache-2.0. The rows above do
not provide an MIT/Apache-only choice, so the build fails the project's stricter
policy even though the licenses are ordinarily considered open source.

DataFusion is therefore not present in any product `Cargo.toml`. Its source is
retained only as the requested Apache-2.0 architecture reference. A future
integration requires either a policy change or an upstream dependency graph
whose complete resolved license set passes the gate.
