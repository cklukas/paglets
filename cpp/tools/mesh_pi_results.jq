# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# A completed/failed Pi status and the controller's file/poll observations.
# Job durations are reported by the jobs; polling does not time their phases.
def integer($name):
  if type == "number" and isfinite and . >= 0 and . == floor then .
  else error($name + " must be a nonnegative integer") end;
def value($name; $v; $unit): {name: $name, value: ($v | tostring), unit: $unit};

(.compute_ms | integer("compute_ms")) as $compute
| (.convert_ms | integer("convert_ms")) as $convert
| (.chunks_done | integer("chunks_done")) as $chunks
| (.chunks_aot | integer("chunks_aot")) as $aot
| ($hex_bytes | integer("hex_bytes")) as $hex
| ($decimal_bytes | integer("decimal_bytes")) as $decimal
| ($hold_ms | integer("hold_ms")) as $hold
| ($began | integer("began")) as $start
| ($computed | integer("computed")) as $hex_seen
| ($ended | integer("ended")) as $end
| if $aot > $chunks then error("chunks_aot exceeds chunks_done") else . end
| if $hex_seen < $start or $end < $hex_seen then error("poll timestamps went backwards") else . end
| if (.hosts | type) != "array" then error("hosts must be an array")
  elif any(.hosts[]; type != "string") then error("host names must be strings") else . end
| [value("hex digits in the file"; $hex; ""),
   value("decimal digits in the file"; $decimal; ""),
   value("minimum chunk hold"; $hold; "ms"),
   value("chunks of 500 digits"; $chunks; ""),
   value("chunks compiled ahead of time"; $aot; ""),
   value("hosts that computed chunks"; (.hosts | join(", ")); ""),
   value("successful hex jobs: reported elapsed time (sum)"; $compute; "ms"),
   value("successful conversion job: reported elapsed time"; $convert; "ms"),
   value("hex completion first observed by polling"; ($hex_seen - $start); "s"),
   value("end first observed after hex completion"; ($end - $hex_seen); "s"),
   value("poll sleep interval"; 1; "s"),
   value("poll clock resolution"; 1; "s")]
