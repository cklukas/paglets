# Paglets Application Note Branch

This orphan branch contains only the LaTeX source and GitHub Actions workflow
for the Paglets application note. It intentionally omits the Python package
source tree from the main project branch.

Build locally:

```bash
make -C papers/paglets-application-note
```

GitHub Actions builds the same PDF and uploads it as the
`paglets-application-note` workflow artifact.
