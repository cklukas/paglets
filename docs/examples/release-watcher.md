# Release Watcher

The Release Watcher keeps track of new versions of tools for machines
without internet access. It moves to a host that may fetch web pages,
stays there, and checks release pages or feeds at a fixed interval. When a
new version appears, it tells its owner. On request, it starts a
[Download Courier](download-courier.md) that brings the release file to
the host the watcher started on.

Directory: [`cpp/examples/watcher/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/watcher)

## What it shows

- **A long-lived paglet, inactive between checks.** After each round of
  checks, the watcher deactivates itself: the host stores its memory
  image, unloads it, and wakes it when the interval is over. The versions
  it saw are ordinary members of its memory, so nothing is lost while it
  is inactive (see
  [Lifecycle, children and timers](../sdk-reference.md#lifecycle-children-and-timers)).
- **The web gateway.** It fetches the pages through the `web` system
  paglet of a host started with `--web`, and gets there with the transfer
  ticket `offer:web.fetch`. The gateway checks every URL against the mesh
  policy (see
  [Gateways to the web and to AI](../system-paglets.md#gateways-to-the-web-and-to-ai)).
- **Paglets starting other paglets.** For `fetch`, the watcher creates a
  Download Courier as its child, from the courier's module hash, and gives
  it its errand. The courier then goes its own way.
- **Notifications.** New versions reach the owner through the `user-info`
  system paglet.

## Why it is useful

Machines in a lab or a production network often have no internet access,
and keeping their tools current means someone has to look for new
releases and copy them in. The watcher does the looking from the one host
that may reach the web, without a cron job or a service of its own, and
costs nothing but a stored memory image between checks. When a release is
wanted, a courier carries it in, verified by its SHA-256.

## How it works

The watcher starts with a `start` message: the pages to watch (a name,
the URL of a release page or feed, and the URL of the release file with
`{version}` in place of the version), the interval, the number of rounds
(0: until `stop`), the transfer ticket to a web host, and the hash of the
Download Courier module. It notes the key of the host it started on, and
moves to a host that offers `web.fetch`. On arrival there, it checks the
pages for the first time.

For each page, it fetches the URL with `web.fetch`, and takes the first
version on the page: in a feed, the first one from its first entry on,
such as `1.4.2` or `v2.0`. A version that differs from the one it saw
before is an update: the watcher records it and tells its owner.
Failures are kept per page, and the other pages are still checked:

<!-- snippet: cpp/examples/watcher/watcher.cpp#check -->
Source: [`cpp/examples/watcher/watcher.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/watcher/watcher.cpp)

```cpp
svc::web::Client client{*web};
svc::web::FetchRequest q;
q.url = watch_.pages[i].url;
auto sent = client.fetch(q, [this, i](paglets::Result<svc::web::FetchReply> r, Message&) {
    auto& seen = status_.seen[i];
    seen.checked_ms = now_ms();
    if (!r || r->status < 200 || r->status >= 300) {
        seen.error = !r ? pt::error_text("fetch", r.error()) : "HTTP status " + std::to_string(r->status);
        return check(i + 1);
    }
    const std::string version =
        version_of(std::string_view(reinterpret_cast<const char*>(r->body.data()), r->body.size()));
    if (version.empty()) {
        seen.error = "no version on the page";
        return check(i + 1);
    }
    seen.error.clear();
    if (!seen.version.empty() && seen.version != version) {
        status_.updates.push_back(Update{seen.name, seen.version, version, seen.checked_ms});
        pt::notify(pt::Level::info, seen.name + " " + version + " is out",
                   "was " + seen.version + "; " + watch_.pages[i].url);
    }
    seen.version = version;
    check(i + 1);
});
```
<!-- /snippet -->

After the last page, the round is done. Unless that was the last round,
the watcher deactivates itself for the interval and counts how often it
was inactive:

<!-- snippet: cpp/examples/watcher/watcher.cpp#sleep -->
Source: [`cpp/examples/watcher/watcher.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/watcher/watcher.cpp)

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
    if (paglets::deactivate(interval)) {
        ++status_.sleeps;
    } else {
        (void)paglets::after_raw(interval, "check", {});
    }
}
```
<!-- /snippet -->

The host wakes it when the interval is over. A message, such as `status`,
wakes it as well. When more than half of the interval is still left, the
watcher then sets a timer for the rest instead of checking early:

<!-- snippet: cpp/examples/watcher/watcher.cpp#wake -->
Source: [`cpp/examples/watcher/watcher.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/watcher/watcher.cpp)

```cpp
// The host wakes it on time. Messages wake it too (`status`, replies):
// then it checks on time with a timer instead.
void on_activated() override {
    if (status_.state != "watching") return;
    const std::int64_t left = next_check_ - now_ms();
    if (left > std::max<std::int64_t>(5, watch_.interval_ms / 2)) {
        (void)paglets::after_raw(left, "check", {});
        return;
    }
    check(0);
}
```
<!-- /snippet -->

`fetch` names a watched page. The watcher fills in the release file's URL
with the latest version (without its leading `v`), creates a Download
Courier from the module hash it was given, and starts it with the URL and
the key of the watcher's starting host as `deliver_to`. The courier,
started on a web host, downloads the file there as an artifact and carries
the artifact capability to that host, which then fetches the content (see
[Moving content between hosts](../system-paglets.md#moving-content-between-hosts)).
The watcher drops its endpoint to the courier and replies with the URL:

<!-- snippet: cpp/examples/watcher/watcher.cpp#courier -->
Source: [`cpp/examples/watcher/watcher.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/watcher/watcher.cpp)

```cpp
void fetch(const Fetch& f, Message& m) {
    const auto page = std::ranges::find_if(watch_.pages, [&](const Page& p) { return p.name == f.name; });
    if (page == watch_.pages.end()) return (void)m.reply(Fetching{false, "not watched: " + f.name, {}});
    const auto& seen = status_.seen[static_cast<std::size_t>(page - watch_.pages.begin())];
    if (seen.version.empty() || page->asset.empty()) {
        return (void)m.reply(Fetching{false, "no version or no release file known", {}});
    }
    if (watch_.courier_module.empty()) return (void)m.reply(Fetching{false, "no courier module", {}});
    const std::string bare = seen.version.front() == 'v' || seen.version.front() == 'V' ? seen.version.substr(1) : seen.version;
    const std::string url = replace_all(page->asset, "{version}", bare);
    auto child = paglets::create_child_raw({}, {}, watch_.courier_module);
    if (!child) return (void)m.reply(Fetching{false, pt::error_text("courier", child.error()), {}});
    auto courier = std::make_shared<paglets::Endpoint>(std::move(*child));
    auto reply = std::make_shared<paglets::Capability>(m.defer_reply());
    CourierStart q;
    q.url = url;
    q.deliver_to = home_;
    auto sent = pt::call<CourierStarted>(*courier, "start", q, [reply, courier, url](paglets::Result<CourierStarted> r) {
        (void)courier->drop();  // the courier goes its own way
        if (!r) return (void)paglets::reply_to(*reply, Fetching{false, pt::error_text("courier", r.error()), url});
        (void)paglets::reply_to(*reply, Fetching{r->accepted, r->reason, url});
    });
    if (!sent) (void)paglets::reply_to(*reply, Fetching{false, pt::error_text("courier", sent.error()), url});
}
```
<!-- /snippet -->

`stop` ends the checks; the state becomes `done`.

The messages and the status. `CourierStart` and `CourierStarted` have the
same fields, in the same order, as the Download Courier's `start` and its
reply:

<!-- snippet: cpp/examples/watcher/watcher_msgs.hpp -->
Source: [`cpp/examples/watcher/watcher_msgs.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/watcher/watcher_msgs.hpp)

```cpp
// Messages of the Release Watcher demo (planning/cpp-demo-paglets.md, demo 22).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace watcher_msgs {

// A release page or feed. The version is the first one on the page (in a
// feed: in its first entry), such as 1.4.2 or v2.0. `asset`: the URL of the
// release file, with {version} for the version (without a leading v).
struct Page {
    std::string name;
    std::string url;
    std::string asset;
};

// `start`: move to a host that offers `web.fetch` and check the pages there
// every `interval_ms` (`checks` times; 0: until `stop`). `courier_module`:
// hex hash of the Download Courier module, for `fetch`.
struct Watch {
    std::vector<Page> pages;
    std::int64_t interval_ms = 3'600'000;
    std::int64_t checks = 0;
    std::string via = "offer:web.fetch";
    std::string courier_module;
};

struct Started {
    bool accepted = false;
    std::string reason;
};

struct Seen {
    std::string name;
    std::string version;
    std::int64_t checked_ms = 0;
    std::string error;  // of the last check
};

struct Update {
    std::string name;
    std::string from;
    std::string to;
    std::int64_t at_ms = 0;
};

struct Status {
    std::string state;  // idle, travelling, watching, done, failed
    std::vector<Seen> seen;
    std::vector<Update> updates;
    std::int64_t checks = 0;
    std::int64_t sleeps = 0;
    std::string host;  // where it watches from
    std::string error;
};

// `fetch`: start a Download Courier for the release file of the page's
// latest version; it brings the file to the host the watcher started on.
struct Fetch {
    std::string name;
};

struct Fetching {
    bool accepted = false;
    std::string reason;
    std::string url;
};

// The Download Courier's `start` and its reply (courier_msgs::Start and
// courier_msgs::Started, the same fields in the same order).
struct CourierStart {
    std::string url;
    std::string sha256;
    std::string via = "offer:web.download";
    std::string deliver_to;
};

struct CourierStarted {
    bool accepted = false;
    std::string reason;
};

}  // namespace watcher_msgs
```
<!-- /snippet -->

## Running it on a live mesh

The mesh needs a host that offers `web`:

```bash
H=build/macos-arm64/host/paglets-host
$H serve --key gw.key --ledger ledger-gw --state state-gw --web --join https://lab-1:7443 &
```

An admin adds a rule that lets paglets fetch the release feed and
download release files. For the service `web`, `--root` is the host name
of a URL and `--path` its path:

```bash
$H ledger rule --ledger ledger --admin alice.key --name releases --decision allow \
    --service web --op fetch --op download --root example.org --path "tool/**"
$H remote push --connect https://lab-1:7443 --key alice.key --ledger ledger
```

The courier is created on the web host, so its module must be there. The
owner stores it with `remote push-module`, which prints its hash:

```bash
COURIER=$($H remote push-module --connect https://gw:7443 --key olga.key --ledger ledger \
    build/macos-arm64/guests/courier.wasm)
```

Then the owner launches the watcher on a host without internet access and
starts it, with a check every hour until `stop`:

```bash
R="--connect https://lab-1:7443 --key olga.key --ledger ledger"
ID=$($H remote launch $R build/macos-arm64/guests/watcher.wasm)
$H remote call $R $ID start '{"pages": [{"name": "tool", "url": "https://example.org/tool/releases.atom",
    "asset": "https://example.org/tool/files/tool-{version}.tar.gz"}],
    "interval_ms": 3600000, "checks": 0, "courier_module": "'$COURIER'"}'
```

The watcher stays on the web host, and `remote call` reaches a paglet on
the host where it is:

```bash
G="--connect https://gw:7443 --key olga.key --ledger ledger"
$H remote call $G $ID status
$H remote call $G $ID fetch '{"name": "tool"}'
$H remote call $G $ID stop
```

`status` lists every page with the version seen, the time of its last
check and its last error, the updates found, and how often the watcher
checked and was inactive. After `fetch`, the courier comes to `lab-1`
with the release file and tells its owner when the file is delivered;
`remote status` on `lab-1` lists it among the host's paglets.

## Test results

The test runs the watcher on a mesh of three hosts inside one test
process (`gateway: Release Watcher ...` in `cpp/tests/test_gateway.cpp`).
Only `gateway` offers `web`, limited to a small web server of the test on
the local machine, which serves an Atom feed and a release file. The test
checks that:

- the watcher, started on `home` with an interval of 100 ms, moves to
  `gateway`, is `watching` there, and has seen `v1.4.2` without an update;
- when the feed announces `v1.5.0`, the watcher records the update from
  `v1.4.2` to `v1.5.0`, was inactive between checks at least once, and its
  owner has the notification `tool v1.5.0 is out`;
- `fetch` starts a Download Courier for `tool-1.5.0.tar.gz`; the courier
  goes from `gateway` to `home` and delivers the file there, with the
  SHA-256 of the served file;
- `stop` ends the watching.

<!-- results: watcher -->
| Test | Mode | Outcome | Duration | Platform | Recorded | checks of the release feed | times inactive between checks | new versions reported | release file delivered home by a Download Courier |
|---|---|---|---|---|---|---|---|---|---|
| gateway: Release Watcher stays on the web host, tells its owner about new versions, sends a courier | in-process | passed | 0.4 s | Darwin arm64, Release build | 2026-10-12 | 2 | 2 | 1 | yes |
| gateway: Release Watcher stays on the web host, tells its owner about new versions, sends a courier | worker processes | passed | 0.9 s | Darwin arm64, Release build | 2026-10-12 | 2 | 2 | 1 | yes |
<!-- /results -->
