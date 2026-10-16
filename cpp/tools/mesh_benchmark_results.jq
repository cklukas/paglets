# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# A controlled benchmark requires the lease the clone reports, rather than
# inferring it from the requested configuration. Preserve rejected samples
# as failed evidence instead of labelling their execution as leased.
[.hosts[] |
 {test: .host_name,
  mode: (if .slot == true then "in a compute slot, worker processes"
         else "without a compute slot, worker processes" end),
  outcome: (if .error == "" and .slot == true then "passed" else "failed" end),
  seconds: (.bench_ms / 100 | round / 10),
  platform: "\(.os) \(.architecture), \(.cpus) CPUs",
  build: $builds[.host_name], date: $date,
  values: [{name: "integer", value: (.int_mops | round | tostring), unit: "Mops"},
           {name: "floating point", value: (.float_mflops | round | tostring), unit: "Mflops"},
           {name: "memory copying", value: (.memory_mbs | round | tostring), unit: "MB/s"},
           {name: "storage write, 8 MB", value: (.disk_write_mbs | round | tostring), unit: "MB/s"},
           {name: "storage read, 8 MB", value: (.disk_read_mbs | round | tostring), unit: "MB/s"},
           {name: "compute slot held", value: ((.slot == true) | tostring), unit: ""}]}]
