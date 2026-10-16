# Agent Instructions

## Private history and GitHub publication

This development repository contains private paper history. Push development
branches only to the rpi4 remotes. Never push this repository to GitHub, including
via a directly typed URL, force push, mirror, tag, or `--no-verify`. Do not disable
or override its installed pre-push guard or disabled `origin` push URL.

Publish future Python and C++ code, documentation and paper updates through the separate
public-only clone using the checked snapshot workflow in the private file
`planning/publication/README.md`.
Use `python3 -B planning/publication/publish.py prepare cpp` (or `main` / `cpp-paper`),
review and validate the public candidate, then use the tool's `push` command.
Never fetch private branches into the public clone. Internal files under
`planning/` are denied by default unless explicitly listed in the publication
policy. Install the guards on new independent development clones before pushing.

This branch (`cpp`) holds only paglets/cpp, the C++ edition. The Python
edition lives on `main`; do not add Python package code, Python tests or
Python documentation here.

## License Headers

New code files must include the project copyright and license notice. For
C and C++ files:

```cpp
// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.
```

For other source-code file types (CMake, shell, YAML), use the equivalent
comment syntax for that language while preserving the same text.

Do not add project license headers to generated files, vendored dependencies,
caches, build outputs, or other ignored artifacts.

## Building and Testing

Build and test from `cpp/` with the CMake presets (`macos-arm64`,
`linux-gcc16`, the sanitizer presets, `windows-mingw-gcc16`):

```bash
cd cpp
cmake --preset macos-arm64
cmake --build --preset macos-arm64
ctest --preset macos-arm64
```

## Documentation

When adding or changing user-visible features, update the documentation under
`docs/`. The site is built with ckdocs (from ck-git-hosting). `ckdocs.yml` at
the repository root describes the whole site: the C++ edition from `docs/` of
this branch and the Python edition from `docs/` of `main`, selected in the
page header. Keep pages within the ckdocs Markdown subset: no raw HTML,
footnotes, definition lists, math, or Mermaid; use `> [!NOTE]`-style alerts,
tables, and lists instead. Check the site with `ckdocs check --root .`.

Code excerpts (`<!-- snippet: PATH#SECTION -->`, from `// --8<-- [start:SECTION]`
markers in the sources) and test results (`<!-- results: TOPIC -->`, from
`docs/results/TOPIC.json`) are generated into the Markdown by
`paglets-docs-sync` (built with the C++ tree); never edit them by hand. Run
`cpp/build/<preset>/tools/docs_sync/paglets-docs-sync --root .` after changing
marked code; the `docs_sync` ctest fails while a page is not current. LaTeX
papers under `papers/` use the same markers as `%` comments
(`% snippet: PATH#SECTION [listings options]` ... `% /snippet`,
`% results: TOPIC [caption={...},label={...}]` ... `% /results`), rendered
as `lstlisting` blocks and booktabs tables.
`cpp/tools/update_results.sh` reruns the example tests and records their
results; `cpp/tools/mesh_bench.sh` records the live benchmarks on real
machines.

Also update the root `README.md` and `cpp/README.md` when the change affects
building, quick-start usage, command-line entry points, major examples, or
public APIs. Keep documentation examples current with the code.

## Units

Use classic binary-scaled byte units for storage, payload sizes, and byte
throughput: label them as `KB`, `MB`, `GB`, etc., and scale by 1024. Do not use
IEC labels such as `KiB`, `MiB`, or `GiB`.

For network-style bit throughput, use decimal-scaled units such as `kbit/s`,
`Mbit/s`, and `Gbit/s`, scaled by 1000.
