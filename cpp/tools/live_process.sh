# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.

# Sourced by live drivers. The caller provides local_name and on HOST COMMAND.
# The marker is an exact, space-delimited argument sequence identifying the
# run's workspace. Remote hosts must expose Linux /proc.
# 0: recorded process and workspace match; 1: PID has exited; 2: identity
# cannot be established. A failed SSH connection is never evidence of exit.
live_process_state() {
    local h="$1" pid="$2" expected="$3" marker="$4" actual args result
    [ -n "$pid" ] || return 1
    [[ "$pid" =~ ^[0-9]+$ ]] || return 2
    if [ "$h" = "$local_name" ]; then
        if [ "$(uname -s)" = Darwin ]; then
            if actual="$(ps -p "$pid" -o comm= 2>/dev/null)"; then :;
            else
                local status=$?
                [ "$status" = 1 ] && return 1
                return 2
            fi
        else
            [ -d "/proc/$pid" ] || return 1
            case "$(ps -p "$pid" -o stat=)" in Z*) return 1;; esac
            actual="$(readlink "/proc/$pid/exe" 2>/dev/null)" || return 2
        fi
        args="$(ps -p "$pid" -o args= 2>/dev/null)" || return 2
    else
        result="$(on "$h" "if [ ! -d /proc/$pid ]; then echo exited; else case \$(ps -p $pid -o stat=) in Z*) echo exited;; *) readlink /proc/$pid/exe && tr '\\000' ' ' < /proc/$pid/cmdline;; esac; fi" 2>/dev/null)" || return 2
        [ "$result" != exited ] || return 1
        actual="${result%%$'\n'*}"
        args="${result#*$'\n'}"
    fi
    [ "$actual" = "$expected" ] && [[ " $args " == *" $marker "* ]] || return 2
}
