# Package Releases

`paglets` is a standard Python package built from `pyproject.toml` with
Hatchling. The repository builds both a wheel and source distribution.

## Local Build

Build the package from the repository root:

```bash
uv build
```

The build writes artifacts under `dist/`. The CI and publish workflows also
install the built wheel into a clean virtual environment and run command-line
smoke checks against the installed package.

## Release Workflow

`.github/workflows/publish.yml` is the package release workflow.

It runs on:

- pushed tags matching `v*.*.*`;
- manual `workflow_dispatch`, for build verification without publishing.

For tag runs, the workflow verifies that the tag version matches the
`project.version` value in `pyproject.toml`. For example, package version
`0.1.0` must be released with tag `v0.1.0`.

The build job runs:

```bash
uv sync --dev --extra docs
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
uv run --extra docs mkdocs build --strict
uv build
uvx twine check --strict dist/*
```

It then installs the built wheel and smoke-tests every packaged console script.

## Publishing To PyPI

The publish job uses PyPI trusted publishing through GitHub Actions OIDC. Before
the first real release, configure the `paglets` project on PyPI with a trusted
publisher for:

- repository: `cklukas/paglets`;
- workflow: `publish.yml`;
- environment: `pypi`.

Once that is configured, pushing a matching version tag publishes the package:

```bash
git tag v0.1.0
git push origin v0.1.0
```

## Related Pages

- [Host CLI](host-cli.md) lists the packaged command-line entry points.
- [Configuration](configuration.md) covers bundled launch defaults included in
  the package.
