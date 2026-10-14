#!/bin/sh
# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
#
# Extracts the data of the figures from the recorded results:
#   data.sh RESULTS_DIR OUT_DIR
# writes OUT_DIR/*.tsv for the gnuplot scripts in figures/.
set -eu
results=$1
out=$2
mkdir -p "$out"
