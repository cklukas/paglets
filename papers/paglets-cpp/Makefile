# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# The paper on the C++ edition. Code excerpts and result tables in the .tex
# files are kept current by paglets-docs-sync (see ../../AGENTS.md); the
# figures are drawn by gnuplot from data that figures/data.sh extracts from
# ../../docs/results/*.json.

TEX = paglets-cpp.tex
PDF = build/paglets-cpp.pdf
CHAPTERS = $(wildcard chapters/*.tex)
GNUPLOTS = $(wildcard figures/*.gnuplot)
FIGURES = $(patsubst figures/%.gnuplot,build/figures/%.pdf,$(GNUPLOTS))
RESULTS = $(wildcard ../../docs/results/*.json)

.PHONY: all clean distclean open sync check

all: $(PDF)

$(PDF): $(TEX) $(CHAPTERS) references.bib $(FIGURES)
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

open: $(PDF)
	open $(PDF)

clean:
	latexmk -c -outdir=build $(TEX)

distclean:
	rm -rf build
