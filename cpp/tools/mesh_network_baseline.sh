# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# Sourced by mesh_bench.sh. Mesh keys are fresh in every benchmark fixture;
# hash the physical host/OS installation identity instead. Never publish its
# raw machine ID. A missing identity is an error, rather than a shared hash
# of an empty string.
mesh_network_machine() {
    local identity
    case "$(uname -s)" in
        Darwin)
            identity="$(ioreg -rd1 -c IOPlatformExpertDevice | awk -F '"' '/IOPlatformUUID/ {print $(NF-1)}')"
            [ -n "$identity" ] || return 1
            printf '%s' "$identity" | shasum -a 256 | awk '{print $1}' ;;
        Linux)
            identity="$(cat /etc/machine-id)"
            [ -n "$identity" ] || return 1
            printf '%s' "$identity" | sha256sum | awk '{print $1}' ;;
        *) echo 'network baseline identity requires macOS or Linux' >&2; return 1 ;;
    esac
}

# A baseline belongs to an ordered pair of machine identities and endpoints,
# not just display names. Return result-value rows or [] if that pair has no
# usable measurement. Invalid files are errors.
mesh_network_values() {
    local file="$1" topology="$2" from="$3" to="$4"
    if [ ! -f "$file" ]; then printf '[]\n'; return; fi
    jq -c --argjson hosts "$topology" --arg from "$from" --arg to "$to" '
        if .baseline_version != 1 or (.tests | type) != "array" then
            error("unsupported network baseline format")
        else
            [.tests[] | select(.outcome == "passed"
                and .from.name == $from and .to.name == $to
                and .from.machine == $hosts[$from].machine and .to.machine == $hosts[$to].machine
                and .from.url == $hosts[$from].url and .to.url == $hosts[$to].url
                and (.from.machine | type) == "string" and (.from.machine | length) > 0
                and (.to.machine | type) == "string" and (.to.machine | length) > 0
                and (.from.url | type) == "string" and (.from.url | length) > 0
                and (.to.url | type) == "string" and (.to.url | length) > 0
                and (.date | type) == "string" and (.date | length) > 0)
             | . as $row | .values[]
             | select(.name == "TCP between the hosts (iperf3)" and .unit == "Mbit/s"
                 and ((.value | tonumber?) // 0) > 0)
             | [., {name: "TCP baseline recorded", value: $row.date, unit: ""}]]
            | last // []
        end' "$file"
}
