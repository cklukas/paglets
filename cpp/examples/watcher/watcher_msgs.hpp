// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

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
