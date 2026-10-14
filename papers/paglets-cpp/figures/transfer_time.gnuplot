# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
#
# Time the destination host took to save a File Courier's payload (fetch,
# SHA-256 check, write) on the live mesh, by payload size and ordered pair
# of hosts (build/data/transfer_time.tsv from figures/data.sh).
# Run from the paper directory: gnuplot figures/transfer_time.gnuplot
set terminal pdfcairo size 12cm,7cm font ",9" enhanced
set output "build/figures/transfer_time.pdf"
set datafile separator "\t"
set key autotitle columnhead top left Left reverse box opaque samplen 2 font ",8"
set logscale x 2
set logscale y 10
set xtics (16, 64, 256)
set xrange [12:340]
set yrange [0.3:20]
set ytics (0.5, 1, 2, 5, 10)
set xlabel "payload (MB)"
set ylabel "saving on the destination (s)"
set grid ytics lc rgb "#B8C7D3" lw 0.5
set border 3 lc rgb "#5F6B76"
set tics nomirror
set style line 1 lc rgb "#1F5A85" lw 1.5 pt 7 ps 0.6
set style line 2 lc rgb "#1F5A85" lw 1.5 pt 6 ps 0.6 dt 2
set style line 3 lc rgb "#1B7F4B" lw 1.5 pt 5 ps 0.6
set style line 4 lc rgb "#1B7F4B" lw 1.5 pt 4 ps 0.6 dt 2
set style line 5 lc rgb "#A86400" lw 1.5 pt 9 ps 0.6
set style line 6 lc rgb "#A86400" lw 1.5 pt 8 ps 0.6 dt 2
plot for [i=2:7] "build/data/transfer_time.tsv" using 1:i with linespoints ls (i-1)
