# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
#
# Round trips of a request between Latency Map probes on the live mesh,
# per ordered pair of hosts: the mean of 20 pings and the fastest
# (build/data/latency.tsv from figures/data.sh).
# Run from the paper directory: gnuplot figures/latency.gnuplot
set terminal pdfcairo size 12cm,7cm font ",9" enhanced
set output "build/figures/latency.pdf"
set datafile separator "\t"
set style data histogram
set style histogram clustered gap 1
set style fill solid 0.9 border lc rgb "#5F6B76"
set boxwidth 0.8
set key top right box opaque samplen 2 font ",8"
set yrange [0:6]
set ylabel "round trip (ms)"
set grid ytics lc rgb "#B8C7D3" lw 0.5
set border 3 lc rgb "#5F6B76"
set tics nomirror
set xtics font ",8" rotate by -30
set bmargin 4
plot "build/data/latency.tsv" using 2:xtic(1) title "mean of 20" lc rgb "#1F5A85", \
     "" using 3 title "fastest" lc rgb "#B8C7D3"
