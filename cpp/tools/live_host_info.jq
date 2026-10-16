# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# Parse the actual executable's --info output; no guessed build defaults.
def field($name):
    $info | split("\n")
    | map(select(test("^\\s*" + $name + ":"))
          | sub("^\\s*" + $name + ":\\s*"; "") | sub("\r$"; ""))
    | first // "";
{name: $host, platform: field("platform"), build: field("build"),
 compiler: field("compiler"), runtime: field("runtime"), info: $info}
| if any([.name, .platform, .build, .compiler, .runtime][]; . == "")
  then error("host --info lacks required build/platform/compiler/runtime metadata") else . end
