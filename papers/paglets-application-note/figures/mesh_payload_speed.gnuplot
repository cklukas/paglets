# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

set terminal pdfcairo enhanced color size 5.2in,3.2in font "Helvetica,9"
set output "build/figures/mesh_payload_speed.pdf"
set datafile separator "\t"
set key inside top left vertical Left reverse opaque box samplen 2.4 spacing 1.45 font "Helvetica,8"
set grid ytics lc rgb "#dddddd"
set logscale x 10
set xtics ("1" 1048576, "4" 4194304, "16" 16777216, "64" 67108864, "256" 268435456, "512" 536870912, "1024" 1073741824)
set xlabel "Payload size (MB)"
set ylabel "Effective payload speed (MB/s)"
set xrange [800000:1400000000]
set yrange [0:*]
plot \
  "build/mesh_payload_speed.tsv" using 1:2 with linespoints lw 2.2 pt 7 ps 0.7 lc rgb "#7b2cbf" title "self/mac", \
  "build/mesh_payload_speed.tsv" using 1:3 with linespoints lw 2.2 pt 5 ps 0.7 lc rgb "#009e73" title "self/windows", \
  "build/mesh_payload_speed.tsv" using 1:4 with linespoints lw 2.2 pt 9 ps 0.7 lc rgb "#56b4e9" title "other/mac", \
  "build/mesh_payload_speed.tsv" using 1:5 with linespoints lw 2.2 pt 11 ps 0.7 lc rgb "#e69f00" title "other/windows"
