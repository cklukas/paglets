#!/bin/sh
# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
#
# Compiles one chapter on its own (with the paper's preamble), so a chapter
# can be checked while the others are written:
#   tools/check-chapter.sh NAME [OUTDIR]     # chapters/NAME.tex
# References to other chapters show as ?? (a warning, not an error).
set -eu
name=$1
out=${2:-build/check-$name}
paper=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$out"
cat > "$out/check.tex" <<TEX
\\input{$paper/preamble}
\\begin{document}
\\section{Chapter $name}
\\input{$paper/chapters/$name}
\\bibliographystyle{plain}
\\bibliography{$paper/references}
\\end{document}
TEX
cd "$out"
TEXINPUTS="$paper:$paper/chapters:$paper/build:" BIBINPUTS="$paper:" latexmk -pdf -interaction=nonstopmode -halt-on-error -quiet check.tex >/dev/null 2>&1 || {
    echo "chapter $name does not compile; errors from $out/check.log:" >&2
    grep -n -A3 "^!" check.log | head -40 >&2
    exit 1
}
grep -E "Warning.*(undefined|multiply)" check.log | grep -v "Citation\|Reference \`sec:\|Reference \`fig:\|Reference \`lst:\|Reference \`tab:\|Reference \`app:" | head -20 >&2 || true
echo "chapter $name compiles: $out/check.pdf"
