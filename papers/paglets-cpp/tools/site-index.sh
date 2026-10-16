#!/bin/sh
# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
#
# Writes the index page of the review site (make site): the paper's title,
# its build date, page count and commit, and the link to the PDF.
#   site-index.sh PDF > index.html
set -eu
pdf=$1
pages=$(pdfinfo "$pdf" 2>/dev/null | sed -n 's/^Pages: *//p')
commit=$(git rev-parse --short=12 HEAD 2>/dev/null || echo "unknown")
date=$(date -u +%Y-%m-%d\ %H:%M\ UTC)
cat <<HTML
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Paglets: the C++ edition, the paper</title>
<style>
body { font: 16px/1.5 system-ui, sans-serif; max-width: 42rem; margin: 3rem auto; padding: 0 1rem; color: #1f2933; }
h1 { font-size: 1.5rem; color: #1f5a85; }
a { color: #1f5a85; }
.meta { color: #5f6b76; font-size: 0.9rem; }
.button { display: inline-block; padding: 0.5rem 1rem; border: 1px solid #b8c7d3; border-radius: 6px; text-decoration: none; margin-top: 1rem; }
</style>
</head>
<body>
<h1>Paglets: Mobile Agents as Migrating WebAssembly Instances</h1>
<p>The paper on the C++ edition of paglets, built from the <code>cpp-paper</code> branch. This is the
current version.</p>
<p class="meta">Built $date from commit <code>$commit</code>; ${pages:-?} pages.</p>
<p><a class="button" href="paglets-cpp.pdf">Open the PDF</a></p>
<p class="meta">Sources: <code>papers/paglets-cpp/</code>; plan and refinement list: <code>planning/cpp-paper.md</code>.</p>
</body>
</html>
HTML
