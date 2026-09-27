# Licensing policy

World Model DB source is licensed under Apache License 2.0. Project policy is
stricter than general open-source compatibility: incorporated or runtime-linked
third-party software must be licensed under MIT, Apache-2.0, or a clearly stated
dual license that permits use under either MIT or Apache-2.0.

## Prohibited licenses

Do not add GPL, LGPL, AGPL, SSPL, BSL, PolyForm, Elastic License, Commons Clause,
source-available, evaluation-only, non-commercial, field-of-use-restricted, or
commercial-only code or dependencies. Do not copy code merely because it is
visible on the internet.

## Review procedure

Before merging a dependency:

1. pin the exact version or commit and record its canonical repository;
2. read the license file in that version, not only a registry label;
3. inspect every transitive runtime and build dependency;
4. verify feature flags do not pull prohibited optional components;
5. record project, repository, version, license, purpose, and whether code is
   used or only studied in `THIRD_PARTY_LICENSES.md`;
6. retain required notices and attribution;
7. run the repository's license audit and review any ambiguous result manually.

An SPDX expression containing `OR` is acceptable only when MIT or Apache-2.0 is
a choice available to this project. `AND` requires every term to be acceptable.
Unknown, missing, custom, or unparseable licenses fail closed.

## References versus dependencies

Architecture documents may discuss other systems without incorporating them.
Studying an implementation is still recorded when it materially informs this
codebase, with `studied only` in the usage column. Do not copy reference code.

Apache Arrow, DataFusion, Iceberg, OpenDAL, Kùzu, FAISS, and SQLite are not V0
dependencies merely because they appear in design discussion. Each must pass a
fresh version-specific review before use.

## Contributions

By contributing, authors certify that they have the right to submit their work
under the repository license and that new third-party material is fully disclosed.
Generated files must identify their generator and its license when the generated
output contains copyrightable material from that generator.

