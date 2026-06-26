# Agent Instructions

## License Headers

New code files must include the project copyright and license notice.

For Python files, use this exact header at the top of the file:

```python
# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
```

For other source-code file types, use the equivalent comment syntax for that
language while preserving the same text.

Do not add project license headers to generated files, vendored dependencies,
virtual environments, caches, build outputs, or other ignored artifacts.

## Documentation

When adding or changing user-visible features, update the MkDocs documentation
under `docs/`.

Also update root-level `README.md` when the change affects installation,
quick-start usage, command-line entry points, major examples, public APIs, or
project-level behavior.

Keep documentation examples current with the code and prefer commands that can
be run from the repository root.

## Release Process

When asked to prepare or publish a new package release, complete the full
technical release sequence. Do not tag before all release metadata changes are
committed and pushed.

1. Inspect the current release state:

   ```bash
   git status --short
   git tag --sort=-version:refname
   git log --oneline <last-tag>..HEAD
   ```

   If there is no previous tag, inspect the full history with
   `git log --oneline`.

2. Choose the next version number deliberately:
   - patch version for compatible fixes and documentation/tooling polish;
   - minor version for new compatible features or meaningful public additions;
   - major version for breaking public API, CLI, package layout, config, or
     behavior changes.

   The project version in `pyproject.toml` and the release tag must match. For
   example, `version = "0.2.0"` is released with tag `v0.2.0`.

3. Update `CHANGES.md`. If it does not exist yet, create it. Build the release
   notes from the git commits since the last version tag:

   ```bash
   git log --oneline <last-tag>..HEAD
   git log --no-merges --format='- %s' <last-tag>..HEAD
   ```

   Analyze the commit messages rather than copying them blindly. Group entries
   by user-visible area where useful, such as Added, Changed, Fixed,
   Documentation, Tooling, Packaging, or Internal. Mention breaking changes
   explicitly.

4. Update release metadata:
   - bump `project.version` in `pyproject.toml`;
   - update `uv.lock` if dependency metadata changes require it;
   - update README or MkDocs pages if installation, CLI commands, public APIs,
     or release behavior changed.

5. Run the release verification gates from the repository root:

   ```bash
   uv run ruff check .
   uv run ruff format --check .
   uv run pyright
   uv run pytest
   uv run --extra docs mkdocs build --strict
   uv build
   uvx twine check --strict dist/*
   ```

   Also smoke-test the built wheel in a clean temporary environment and run the
   packaged CLI `--help` commands before tagging.

6. Commit and push the release preparation:

   ```bash
   git add -A
   git commit -m "Release <version>"
   git push origin main
   ```

7. Only after the release commit is pushed, create and push the matching tag:

   ```bash
   git tag v<version>
   git push origin v<version>
   ```

   The tag triggers `.github/workflows/publish.yml`. Watch the publish workflow
   and verify the PyPI project page after it completes.

## Units

Use classic binary-scaled byte units for storage, payload sizes, and byte
throughput: label them as `KB`, `MB`, `GB`, etc., and scale by 1024. Do not use
IEC labels such as `KiB`, `MiB`, or `GiB`.

For network-style bit throughput, use decimal-scaled units such as `kbit/s`,
`Mbit/s`, and `Gbit/s`, scaled by 1000.

## AQRE Live Service Notes

- The live AQRE DS-DIA host service is `paglets-aqre-main.service` and runs from
  `/home/klukasc/ds-dia-install-check` with the interpreter at
  `/home/klukasc/ds-dia-install-check/.venv/bin/python`.
- That service imports Paglets from this checkout:
  `/home/klukasc/ds-dia-install-check/paglets/paglets/src/paglets`. Restarting
  `paglets-aqre-main.service` loads current working-tree Paglets code.
- The service unit stores the API key in `Environment=PAGLETS_API_KEY=...`; do
  not print the key. To run authenticated local checks without exposing it, read
  it into the process environment, for example:

  ```bash
  export PAGLETS_API_KEY="$(sed -n 's/^Environment=PAGLETS_API_KEY=//p' /home/klukasc/.config/systemd/user/paglets-aqre-main.service)"
  /home/klukasc/ds-dia-install-check/.venv/bin/paglets-compute-slots status --queue --jobs
  ```

- The Windows laptop view goes through the public API at
  `https://aqre.ap.basf.net/paglets`; local `pgrep` can briefly see child
  processes that are not yet reflected in the public API. Prefer the public API
  or `paglets-compute-slots` for scheduler/inventory truth.
- `paglets-state/work` is Paglets-owned runtime work storage. It is cleared on
  host startup by `Host._clear_work_root()`. Do not confuse it with application
  scratch directories outside Paglets persistence.
- Compute jobs waiting for slots are inactive paglets. Paglet-facing proxy lookup
  must be able to return inactive local proxies so `send(...,
 activate_if_inactive=True)` can wake them. Avoid fixing this only in one
  caller such as `compute-slots`; keep the behavior at the proxy/context
  abstraction boundary.
