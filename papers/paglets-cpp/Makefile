# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# The paper on the C++ edition. Code excerpts and result tables in the .tex
# files are kept current by paglets-docs-sync (see ../../AGENTS.md); the
# current diagrams are drawn by TikZ. Historical gnuplot recipes and their
# data extractor remain available, but are not inputs of this paper.

TEX = paglets-cpp.tex
PDF = build/paglets-cpp.pdf
CHAPTERS = $(wildcard chapters/*.tex)
GNUPLOTS = $(wildcard figures/*.gnuplot)
FIGURES = $(patsubst figures/%.gnuplot,build/figures/%.pdf,$(GNUPLOTS))
RESULTS = $(wildcard ../../docs/results/*.json)

.PHONY: all clean distclean open sync check check-data site

all: $(PDF)

$(PDF): $(TEX) preamble.tex $(CHAPTERS) references.bib
	latexmk -pdf -interaction=nonstopmode -halt-on-error -outdir=build $(TEX)

build/data.stamp: figures/data.sh $(RESULTS)
	mkdir -p build/data
	sh figures/data.sh ../../docs/results build/data
	touch $@

build/figures/%.pdf: figures/%.gnuplot build/data.stamp
	mkdir -p build/figures
	gnuplot $<

# The code excerpts and result tables, from the sources (needs a build of
# the C++ tree: PAGLETS_BUILD names its directory).
PAGLETS_BUILD ?= ../../cpp/build/macos-arm64
sync:
	$(PAGLETS_BUILD)/tools/docs_sync/paglets-docs-sync --root ../..
check:
	$(PAGLETS_BUILD)/tools/docs_sync/paglets-docs-sync --root ../.. --check

# Supply an explicit disposable workspace (on Mac, below the mounted temp volume).
check-data:
	@test -n "$(TEST_WORK)" || { echo "set TEST_WORK to a disposable directory" >&2; exit 2; }
	sh tools/test-figure-data.sh figures/data.sh "$(TEST_WORK)"

# A small site with the PDF, for the review copy on the ck-git Pages
# server (rpi4): build/site/index.html and the PDF.
site: $(PDF)
	mkdir -p build/site
	cp $(PDF) build/site/paglets-cpp.pdf
	sh tools/site-index.sh $(PDF) > build/site/index.html

open: $(PDF)
	open $(PDF)

clean:
	latexmk -c -outdir=build $(TEX)

distclean:
	rm -rf build
