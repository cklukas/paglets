# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

TEX = paglets-application-note.tex
PDF = build/paglets-application-note.pdf
DATA_DIR = data
FIGURE_DIR = build/figures
MESH_JSON = \
	$(DATA_DIR)/mesh_results_0.json \
	$(DATA_DIR)/mesh_results_1048576.json \
	$(DATA_DIR)/mesh_results_4194304.json \
	$(DATA_DIR)/mesh_results_16777216.json \
	$(DATA_DIR)/mesh_results_67108864.json \
	$(DATA_DIR)/mesh_results_268435456.json \
	$(DATA_DIR)/mesh_results_536870912.json \
	$(DATA_DIR)/mesh_results_1073741824.json
PERF_JSON = $(DATA_DIR)/perf_test.json
MANIFEST_JSON = $(DATA_DIR)/manifest.json
MESH_LATENCY_TSV = build/mesh_latency.tsv
MESH_SPEED_TSV = build/mesh_payload_speed.tsv
FIGURES = \
	$(FIGURE_DIR)/mesh_latency.pdf \
	$(FIGURE_DIR)/mesh_payload_speed.pdf

.PHONY: all clean distclean open

all: $(PDF)

$(PDF): $(TEX) references.bib $(MESH_JSON) $(PERF_JSON) $(MANIFEST_JSON) $(FIGURES)
	latexmk -pdf -interaction=nonstopmode -halt-on-error -outdir=build $(TEX)

$(MESH_LATENCY_TSV): $(MESH_JSON)
	mkdir -p build
	printf "payload_bytes\tmac_to_mac_ms\tmac_to_windows_ms\twindows_to_mac_ms\twindows_to_windows_ms\n" > $@
	for file in $(MESH_JSON); do \
		size=$$(basename "$$file" .json | sed 's/mesh_results_//'); \
		if [ "$$size" = "0" ]; then continue; fi; \
		jq -r --arg size "$$size" '[(($$size) | tonumber), (.matrix_seconds["mac-studio"]["mac-studio"] * 1000), (.matrix_seconds["mac-studio"]["windows11"] * 1000), (.matrix_seconds["windows11"]["mac-studio"] * 1000), (.matrix_seconds["windows11"]["windows11"] * 1000)] | @tsv' "$$file" >> $@; \
	done

$(MESH_SPEED_TSV): $(MESH_JSON)
	mkdir -p build
	printf "payload_bytes\tself_mac_MBps\tself_windows_MBps\tother_mac_MBps\tother_windows_MBps\n" > $@
	for file in $(MESH_JSON); do \
		size=$$(basename "$$file" .json | sed 's/mesh_results_//'); \
		if [ "$$size" = "0" ]; then continue; fi; \
		jq -r --arg size "$$size" 'def speed($$relation; $$host): ([.payload_transfer_speeds[] | select(.relation == $$relation and .host_name == $$host) | (.bytes_per_second / 1024 / 1024)][0] // 0); [($$size | tonumber), speed("self"; "mac-studio"), speed("self"; "windows11"), speed("other"; "mac-studio"), speed("other"; "windows11")] | @tsv' "$$file" >> $@; \
	done

$(FIGURE_DIR)/mesh_latency.pdf: figures/mesh_latency.gnuplot $(MESH_LATENCY_TSV)
	mkdir -p $(FIGURE_DIR)
	gnuplot figures/mesh_latency.gnuplot

$(FIGURE_DIR)/mesh_payload_speed.pdf: figures/mesh_payload_speed.gnuplot $(MESH_SPEED_TSV)
	mkdir -p $(FIGURE_DIR)
	gnuplot figures/mesh_payload_speed.gnuplot

open: $(PDF)
	open $(PDF)

clean:
	latexmk -c -outdir=build $(TEX)

distclean:
	latexmk -C -outdir=build $(TEX)
	rm -rf build
