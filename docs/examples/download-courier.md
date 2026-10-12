# Download Courier

The Download Courier fetches a file from the web for a host that has no
internet access. It starts on that host, moves to a host that offers web
downloads, has the file downloaded there, and comes home with an artifact
capability for it. At home, the home host fetches the content from the web
host and checks it, and the owner gets a notification.

Directory: [`cpp/examples/courier/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/courier)

## What it shows

- **The web gateway.** Paglets never get sockets. A host started with
  `--web` runs the `web` system paglet, which fetches on their behalf: it
  checks every URL against its allowed destinations and the mesh policy,
  and refuses internal addresses unless they are configured (see
  [Gateways to the web and to AI](../system-paglets.md#gateways-to-the-web-and-to-ai)).
- **Moving to an offer.** The courier does not need to know which host
  has internet access. It moves with the transfer ticket
  `offer:web.download`, and the mesh finds a host whose `web` service
  offers `download` (see
  [Service offers](../system-paglets.md#service-offers)).
- **Artifact capabilities instead of content.** `download` stores the file
  on the web host as an artifact and replies with an artifact capability
  of a few bytes. Only the capability travels with the courier. The home
  host fetches the content from the web host when the courier asks its
  `artifacts` service about the artifact, and checks its SHA-256 (see
  [Moving content between hosts](../system-paglets.md#moving-content-between-hosts)).
  A paglet moves as a memory image of at most 16 MB, but the size of the
  file is limited only by the web host's download limit (64 MB by
  default).
- **Notifications.** The courier tells its owner through `user-info`
  when the file has arrived, or, back home, why it failed. Its `status`
  says how far it got.

## Why it is useful

Machines that must not reach the internet, such as lab instruments,
build machines or servers in a protected network, still need files from
it: releases, data sets, certificates. With the courier, one host of the
mesh has web access, the mesh policy decides which sites may be fetched,
and every other host gets the files through the mesh. Nobody copies files
by hand, and the content is checked by SHA-256 on its way, also against a
hash the owner gives in advance.

## How it works

The courier starts at home with a `start` message, which names the URL,
optionally the expected SHA-256, the transfer ticket to the web host, and
optionally another host to deliver to. It asks `mesh-info` where it is,
records that host as home, and moves on with the ticket. If the home host
offers `web.download` itself, it downloads right there:

<!-- snippet: cpp/examples/courier/courier.cpp#depart -->
Source: [`cpp/examples/courier/courier.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/courier/courier.cpp)

```cpp
auto sent = client.snapshot({}, [this, reply](paglets::Result<svc::mesh_info::Snapshot> snap, Message&) {
    if (!snap) {
        status_.state = "failed";
        return (void)paglets::reply_to(*reply, Started{false, error_text("mesh-info", snap.error())});
    }
    here_ = snap->host;
    status_.home = params_.deliver_to.empty() ? snap->host : params_.deliver_to;
    status_.hosts.push_back(snap->host_name.empty() ? snap->host.substr(0, 8) : snap->host_name);
    // Started on a web host (by a paglet staying there): it downloads here.
    const bool web_here = std::ranges::any_of(snap->offers, [](const svc::mesh_info::Offer& o) {
        return o.service == "web" && std::ranges::find(o.ops, "download") != o.ops.end();
    });
    if (web_here && params_.via == "offer:web.download") {
        (void)paglets::reply_to(*reply, Started{true, {}});
        return download();
    }
    if (auto moved = paglets::dispatch(params_.via); !moved) {
        status_.state = "failed";
        status_.error = error_text("dispatch", moved.error());
        return (void)paglets::reply_to(*reply, Started{false, status_.error});
    }
    paglets::reply_to(*reply, Started{true, {}});
});
```
<!-- /snippet -->

On the web host, it looks up the `web` service and asks it to download
the URL. The web host fetches the file, checks the expected hash if one
was given, and stores the content as an artifact. The reply carries the
artifact capability, which the courier keeps. With anything other than an
HTTP status of 2xx, or an error, the courier records the reason and comes
home to report it. Otherwise it goes home; if it cannot move yet, it stays
with the capability and tries again every 2 s:

<!-- snippet: cpp/examples/courier/courier.cpp#download -->
Source: [`cpp/examples/courier/courier.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/courier/courier.cpp)

```cpp
void download() {
    status_.state = "downloading";
    lookup("web", [this](paglets::Result<Endpoint> web) {
        if (!web) return fail_and_return(error_text("web", web.error()));
        svc::web::Client client{*web};
        auto sent = client.download(svc::web::DownloadRequest{params_.url, params_.sha256},
                                    [this](paglets::Result<svc::web::DownloadReply> r, Message& m) {
                                        if (!r) return fail_and_return(error_text("download", r.error()));
                                        status_.http_status = r->status;
                                        if (r->status < 200 || r->status >= 300 || m.cap_count() == 0) {
                                            return fail_and_return("HTTP status " + std::to_string(r->status));
                                        }
                                        blob_ = std::make_shared<Capability>(m.take_cap(0));
                                        status_.size = r->size;
                                        status_.state = "returning";
                                        go_home();
                                    });
        if (!sent) fail_and_return(error_text("download", sent.error()));
    });
}

void go_home() {
    if (here_ == status_.home) return deliver();
    if (auto moved = paglets::dispatch(status_.home); !moved) {
        status_.error = error_text("dispatch home", moved.error());
        (void)paglets::after_raw(2000, "retry-home", {});
    }
}
```
<!-- /snippet -->

At home, the courier lends the capability to the home host's `artifacts`
service and asks for `stat`. The home host does not have the content yet,
so it fetches it from the web host into its own store, checks its
SHA-256, and only then answers. A large file takes a while, so the
request may take up to an hour. Then the courier is `delivered` and
notifies its owner:

<!-- snippet: cpp/examples/courier/courier.cpp#deliver -->
Source: [`cpp/examples/courier/courier.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/courier/courier.cpp)

```cpp
void store_at_home(Endpoint store) {
    svc::artifacts::Client client{store};
    paglets::RequestOptions options = lending(*blob_);
    options.timeout_ms = 3600 * 1000;  // a large download takes a while to arrive
    auto sent = client.stat(
        svc::artifacts::StatRequest{},
        [this](paglets::Result<svc::artifacts::Info> info, Message&) {
            if (!info) return fail(error_text("fetch home", info.error()));
            status_.artifact = info->hash;
            status_.state = "delivered";
            status_.error.clear();
            notify(svc::user_info::Level::success, "courier delivered " + status_.url,
                   "artifact " + info->hash + ", " + std::to_string(status_.size) + " bytes");
        },
        options);
    if (!sent) fail(error_text("fetch home", sent.error()));
}
```
<!-- /snippet -->

The messages and the status:

<!-- snippet: cpp/examples/courier/courier_msgs.hpp -->
Source: [`cpp/examples/courier/courier_msgs.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/courier/courier_msgs.hpp)

```cpp
// Messages of the Download Courier demo (planning/cpp-web-ai.md, section 5).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace courier_msgs {

// `start`: fetch `url` through a host offering `web` and bring it home.
struct Start {
    std::string url;
    std::string sha256;                         // expected content (hex); empty: not checked
    std::string via = "offer:web.download";     // the transfer ticket to the web host
    std::string deliver_to;                     // key ID of the host to deliver to; empty: the starting host
};

struct Started {
    bool accepted = false;
    std::string reason;
};

struct StatusRequest {};

// `status`: where the courier is in its errand.
struct Status {
    std::string state;  // idle, travelling, downloading, returning, delivered, failed
    std::string url;
    std::string home;      // key ID of the host it started on
    std::string artifact;  // delivered: hex SHA-256 of the artifact on the home host
    std::int64_t size = 0;
    std::int64_t http_status = 0;
    std::string error;
    std::vector<std::string> hosts;  // names of the hosts it was on, in order
};

}  // namespace courier_msgs
```
<!-- /snippet -->

## Running it on a live mesh

The mesh needs a host started with `--web`, and a policy rule that lets
the owner's paglets download from the site (see
[serve](../cli-reference.md#serve) and [ledger](../cli-reference.md#ledger)).
In this example, `gw.key` is an enrolled host key with its own ledger
copy, as in [Running hosts](../hosts-and-meshes.md#running-hosts):

```bash
H=build/macos-arm64/host/paglets-host
$H serve --key gw.key --ledger ledger-gw --state state-gw --web --join https://lab-1:7443 &
$H ledger rule --ledger ledger --admin alice.key --name downloads --decision allow \
    --service web --op download --root example.org --path "files/**"
$H remote push --connect https://lab-1:7443 --key alice.key --ledger ledger
```

Then launch the courier on the host without internet access and start it:

```bash
R="--connect https://lab-1:7443 --key olga.key --ledger ledger"
ID=$($H remote launch $R build/macos-arm64/guests/courier.wasm)
$H remote call $R $ID start '{"url": "https://example.org/files/tool-1.5.0.tar.gz", "sha256": "",
    "via": "offer:web.download", "deliver_to": ""}'
$H remote call $R $ID status
```

`sha256` is the expected content as 64 lowercase hex digits, or empty to
accept any content. `status` follows the errand: `travelling`,
`downloading`, `returning`, then `delivered` with the size, the HTTP
status, and in `artifact` the SHA-256 of the content now in the home
host's store; or `failed` with the reason in `error`. `hosts` lists the
hosts it was on, in order.

> [!NOTE]
> The content stays in the home host's `artifacts` store, within the
> store's limit (1 GB by default). Paglets on that host read it with
> `artifacts.get`, or write it to a file with `files.save`.

## Test results

The test runs the courier on a mesh of three hosts inside one test
process: `home`, `gateway` and `plain`
(`gateway: Download Courier ...` in `cpp/tests/test_gateway.cpp`). Only
`gateway` offers `web`, limited to the `/files/` pages of a small web site
on `127.0.0.1` and without internet access, and a policy rule allows
`web.download`. The test checks that:

- the courier leaves `home`, is on `gateway`, and comes back;
- it is `delivered`, the hosts on its way are `home`, `gateway`, `home`,
  and the artifact's SHA-256 and size are those of the file on the site;
- the home host now holds the content in its own store, and the owner got
  a notification `courier delivered ...`;
- a second courier, sent for an address outside the allowed pages, is
  refused by `web` with `denied`, comes home, reports `failed` without
  an artifact, and the owner gets a notification `courier failed`.

<!-- results: courier -->
| Test | Mode | Outcome | Duration | Platform | Recorded | hosts on the way | bytes delivered, verified by SHA-256 | intranet address through web: refused |
|---|---|---|---|---|---|---|---|---|
| gateway: Download Courier fetches through the only web host and brings the file home (WP17 exit) | in-process | passed | 0.3 s | Darwin arm64, Release build | 2026-10-12 | 3 | 22 | yes |
| gateway: Download Courier fetches through the only web host and brings the file home (WP17 exit) | worker processes | passed | 0.4 s | Darwin arm64, Release build | 2026-10-12 | 3 | 22 | yes |
<!-- /results -->
