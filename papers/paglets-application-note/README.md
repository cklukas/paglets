# Paglets Application Note

This directory contains a short LaTeX application note introducing `paglets`,
its relationship to Java Aglets, and the main runtime and usage tradeoffs.

Build the PDF from this directory:

```bash
make
```

The generated file is written to:

```text
build/paglets-application-note.pdf
```

The build uses `latexmk` with `pdflatex` and BibTeX. Generated PDF and LaTeX
intermediate files are intentionally ignored.

GitHub Actions builds the paper on pushes to the isolated `overview-paper`
branch. The workflow intentionally lives only on that branch, so manual dispatch
from GitHub's default-branch workflow list is not available.
