# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

if ($hosts | length) == 0 then error("no host configurations recorded")
else
 {topic: "live_chronicle", tests: [{test: $name,
   mode: "LAN, worker processes", outcome: $outcome, seconds: $seconds,
   platform: ($hosts | map("\(.name): \(.platform)") | join("; ")),
   build: ($hosts | map(.build) | unique
           | if length == 1 then .[0] else "mixed; see host configurations" end),
   hosts: $hosts, date: $date, values: $values}]}
end
