# Volume Guard

The Volume Guard watches the volumes of its host and tells its owner when
one runs low on space. It checks at a fixed interval and is inactive in
between: it takes no memory or CPU on the host while it waits, and the
host wakes it when the next check is due.

Directory: [`cpp/examples/guard/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/guard)

## What it shows

- **Inactive paglets that the host wakes on time.** After each check, the
  guard calls `paglets::deactivate` with the time until the next one. The
  host stores the guard's memory image and unloads it; when the time is
  over, it activates the guard again (see
  [Lifecycle, children and timers](../sdk-reference.md#lifecycle-children-and-timers)).
  A stored image also survives a restart of the host.
- **State in memory across deactivation.** The settings, the volumes
  already reported and the time of the next check are ordinary members.
  They are part of the memory image, so the guard continues where it
  stopped.
- **Timers.** A message can wake an inactive paglet too. Then the guard
  sets a timer for the rest of the interval with `paglets::after_raw`.
- **Notifications to the owner.** The guard tells its owner through the
  `user-info` system paglet, with the `notify` pattern (see
  [The patterns library](../writing-paglets.md#the-patterns-library)).
- **System information by policy.** It reads the volumes from the
  `server-info` system paglet, whose `volumes` operation the mesh policy
  must allow.

## Why it is useful

A full disk stops builds, databases and instruments, and it usually fills
up while nobody looks. The Volume Guard is a small watcher that needs no
monitoring system: launch one on each host that matters. It reports each
low volume once, not at every check, and again only after the volume has
recovered and runs low once more. Between checks it costs the host only
the stored image.

## How it works

The guard starts with a `start` message. It names the share of free
space below which a volume counts as low (`min_free_percent`, 10 by
default), the interval between checks (one minute by default), and the
number of checks (0: until stopped). It replies with its status and
checks at once.

A check looks up `server-info` and asks for the volumes of the host. For
each volume, it computes the free share. A volume above the limit is
removed from the set of low volumes. A volume below the limit that is not
in the set yet is added to it; the guard records an alert and notifies
its owner:

<!-- snippet: cpp/examples/guard/guard.cpp#check -->
Source: [`cpp/examples/guard/guard.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/guard/guard.cpp)

```cpp
void check() {
    pt::lookup("server-info", [this](paglets::Result<paglets::Endpoint> ep) {
        if (!ep) return failed(pt::error_text("server-info", ep.error()));
        si::Client c{*ep};
        auto sent = c.volumes({}, [this](paglets::Result<si::Volumes> v, Message&) {
            if (!v) return failed(pt::error_text("volumes", v.error()));
            ++status_.checks;
            for (const auto& vol : v->volumes) {
                if (vol.total == 0) continue;
                const double free = 100.0 * static_cast<double>(vol.available) / static_cast<double>(vol.total);
                if (free >= watch_.min_free_percent) {
                    low_.erase(vol.mount_point);  // recovered
                    continue;
                }
                if (!low_.insert(vol.mount_point).second) continue;  // reported already
                status_.alerts.push_back(Alert{vol.mount_point, free, now_ms()});
                pt::notify(pt::Level::warning, "volume " + vol.mount_point + " is low on space",
                           std::to_string(static_cast<int>(free)) + "% free");
            }
            sleep();
        });
        if (!sent) failed(pt::error_text("volumes", sent.error()));
    });
}
```
<!-- /snippet -->

After a check, the guard goes to sleep. When it has made the number of
checks asked for, it is `done` instead. It notes the time of the next
check and deactivates itself with a wake-up time of one interval. It
counts the times it went inactive in `sleeps`:

<!-- snippet: cpp/examples/guard/guard.cpp#sleep -->
Source: [`cpp/examples/guard/guard.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/guard/guard.cpp)

```cpp
// Inactive until the next check.
void sleep() {
    if (status_.state != "watching") return;
    if (watch_.checks > 0 && status_.checks >= watch_.checks) {
        status_.state = "done";
        return;
    }
    const std::int64_t interval = std::max<std::int64_t>(10, watch_.interval_ms);
    next_check_ = now_ms() + interval;
    if (paglets::deactivate(interval)) ++status_.sleeps;
}
```
<!-- /snippet -->

When the host activates the guard, it checks. Its own timer is not the
only reason it is activated: a message such as `status`, or the reply to
its notification, activates it as well. If the next check is still more
than half an interval away, the guard sets a timer for the rest of the
interval instead; the `check` message of that timer starts the check:

<!-- snippet: cpp/examples/guard/guard.cpp#wake -->
Source: [`cpp/examples/guard/guard.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/guard/guard.cpp)

```cpp
// The host wakes it when it is time, but a message (a reply to its
// notification, `status`) wakes it too: then it checks on time with a
// timer instead.
void on_activated() override {
    if (status_.state != "watching") return;
    // Timers and clocks differ by a little: a wake within half an
    // interval of the time is the host's.
    const std::int64_t left = next_check_ - now_ms();
    if (left > std::max<std::int64_t>(5, watch_.interval_ms / 2)) {
        (void)paglets::after_raw(left, "check", {});
        return;
    }
    check();
}
```
<!-- /snippet -->

`stop` sets the state to `done`. If `server-info` cannot be found, or
its `volumes` operation fails (for example, because the policy denies
it), the state becomes `failed`, with the reason in `error`.

The messages and the status:

<!-- snippet: cpp/examples/guard/guard_msgs.hpp -->
Source: [`cpp/examples/guard/guard_msgs.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/guard/guard_msgs.hpp)

```cpp
// Messages of the Volume Guard demo (planning/cpp-demo-paglets.md, demo 9).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace guard_msgs {

// `start`: check the host's volumes every `interval_ms`, `checks` times
// (0: until stopped); alert when a volume has less than `min_free_percent`
// of its space available.
struct Watch {
    double min_free_percent = 10;
    std::int64_t interval_ms = 60'000;
    std::int64_t checks = 0;
};

struct Alert {
    std::string mount_point;
    double free_percent = 0;
    std::int64_t at_ms = 0;
};

struct Status {
    std::string state;  // idle, watching, done, failed
    std::int64_t checks = 0;
    std::int64_t sleeps = 0;  // times it went inactive until the next check
    std::vector<Alert> alerts;
    std::string error;
};

}  // namespace guard_msgs
```
<!-- /snippet -->

## Running it on a live mesh

The guard needs a policy rule that allows the owner's paglets the
`volumes` operation of `server-info` (see
[Policy](../hosts-and-meshes.md#policy)):

```bash
H=build/macos-arm64/host/paglets-host
$H ledger rule --ledger ledger --admin alice.key --name volumes --decision allow \
    --service server-info --op volumes --owner <olga-key-id>
$H remote push --connect https://lab-1:7443 --key alice.key --ledger ledger
```

The guard watches the host it runs on. Launch it on the host to watch and
start it, here with checks every five minutes and an alert below 15 %
free space:

```bash
R="--connect https://lab-2:7443 --key olga.key --ledger ledger"
ID=$($H remote launch $R build/macos-arm64/guests/guard.wasm)
$H remote call $R $ID start '{"min_free_percent": 15, "interval_ms": 300000, "checks": 0}'
$H remote call $R $ID status
$H remote call $R $ID stop
```

`status` answers with the state, the number of checks and sleeps, and the
alerts with mount point, free share and time. The notifications reach
the owner through `user-info`. Note that `status` itself activates the
guard; it then keeps its schedule with a timer, as described above.

## Test results

The test runs the guard on a single host inside one test process, with a
policy rule that allows `volumes` of `server-info`
(`example: Volume Guard ...` in `cpp/tests/test_examples.cpp`). It starts
the guard with a limit of 101 % free space, so that every volume counts
as low, an interval of 50 ms and three checks. It leaves the guard
undisturbed for a second, since a message would wake it as well, and
checks that:

- the guard is `done` after three checks, and went inactive twice, between
  the checks;
- it raised at least one alert, and each volume only once, although every
  volume was low at every check;
- a notification titled `volume ...` reached the owner.

<!-- results: guard -->
| Test | Mode | Outcome | Duration | Platform | Recorded | checks | times inactive between checks | alerts (each volume once) |
|---|---|---|---|---|---|---|---|---|
| example: Volume Guard sleeps between checks and alerts its owner | in-process | passed | 1.2 s | Darwin arm64, Release build | 2026-10-12 | 3 | 2 | 6 |
| example: Volume Guard sleeps between checks and alerts its owner | worker processes | passed | 1.2 s | Darwin arm64, Release build | 2026-10-12 | 3 | 2 | 6 |
<!-- /results -->
