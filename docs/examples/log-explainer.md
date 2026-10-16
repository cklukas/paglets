# Log Explainer

The Log Explainer turns many log lines from many machines into a short
list of distinct problems, each with an explanation in plain language. It
lets a [Log Scout](log-scout.md) search the logs, groups the lines that
differ only in numbers, IDs or quoted text, has each group classified and
explained on a host that offers `ai`, and comes home to tell its owner.

Directory: [`cpp/examples/explainer/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/explainer)

## What it shows

- **Paglets cooperating.** The explainer does not search logs itself. It
  creates a Log Scout as its child, from the scout's module hash, and talks
  to it through the endpoint it gets: `start`, then `report` until the
  scout is done, then `dispose`. The scout sends its own clones to the
  hosts and collects their excerpts (see
  [Lifecycle, children and timers](../sdk-reference.md#lifecycle-children-and-timers)).
- **Grouping before asking.** Hundreds of lines often show the same few
  problems. The explainer reduces each line to its shape and asks the
  model once per group, not once per line.
- **`ai.classify` and `ai.generate`.** For each group, the `ai` system
  paglet picks one of a list of labels, and writes an explanation. The
  explainer gets to the AI host with the transfer ticket
  `offer:ai.generate` (see [Service offers](../system-paglets.md#service-offers)
  and [Gateways to the web and to AI](../system-paglets.md#gateways-to-the-web-and-to-ai)).
- **Notifications.** Back home, it tells its owner through the
  `user-info` system paglet.

## Why it is useful

When a service fails, its logs on every machine fill with errors, and
most of them are the same message with another number. Reading them all
to find the two or three actual problems takes time. The explainer does
that sorting, and a language model adds a likely cause for each problem
in words an operator can act on. The log hosts need no model, and the AI
host needs no access to the logs.

## How it works

The explainer starts with a `start` message: the hosts to search (empty:
every host), the named root of the logs, a file pattern, the search
patterns, the time window, the hash of the Log Scout module, the transfer
ticket to the AI host, the labels, and the most groups to explain. It
creates the Log Scout as its child on its home host and starts it, with
at most 50 excerpts per host and pattern. It replies when the scout has
accepted:

<!-- snippet: cpp/examples/explainer/explainer.cpp#scout -->
Source: [`cpp/examples/explainer/explainer.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/explainer/explainer.cpp)

```cpp
void start(const Explain& e, Message& m) {
    if (status_.state != "idle") return (void)m.reply(Started{false, "already " + status_.state});
    if (e.root.empty() || e.scout_module.empty()) return (void)m.reply(Started{false, "a root and the scout module are needed"});
    params_ = e;
    auto child = paglets::create_child_raw({}, {}, e.scout_module);
    if (!child) return (void)m.reply(Started{false, pt::error_text("log scout", child.error())});
    scout_ = std::make_shared<paglets::Endpoint>(std::move(*child));
    status_.state = "scouting";
    auto reply = std::make_shared<paglets::Capability>(m.defer_reply());
    ScoutStart q{e.hosts, e.root, e.files, e.patterns, e.window_ms, 50};
    auto sent = pt::call<ScoutStarted>(*scout_, "start", q, [this, reply](paglets::Result<ScoutStarted> r) {
        if (!r || !r->accepted) {
            fail(!r ? pt::error_text("log scout", r.error()) : "log scout: " + r->reason);
            return (void)paglets::reply_to(*reply, Started{false, status_.error});
        }
        (void)paglets::reply_to(*reply, Started{true, {}});
        where([this] {
            home_ = here_;
            poll();
        });
    });
    if (!sent) {
        fail(pt::error_text("log scout", sent.error()));
        (void)paglets::reply_to(*reply, Started{false, status_.error});
    }
}
```
<!-- /snippet -->

Then it asks the scout for its report every 100 ms, until the scout is
done. It groups the excerpts by their shape: the line in lower case,
without a leading timestamp, with numbers, hex IDs, addresses and quoted
text replaced by `#`. For example, `ERROR disk full on /data (42 MB left)`
becomes `error disk full on /data (# mb left)`. Each group keeps the first
line as its example, the number of lines and the hosts they came from.
The largest groups come first, up to `max_groups`. It disposes of the
scout, and moves to the AI host, unless there is nothing to explain:

<!-- snippet: cpp/examples/explainer/explainer.cpp#group -->
Source: [`cpp/examples/explainer/explainer.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/explainer/explainer.cpp)

```cpp
// Asks the scout for its report until it is done.
void poll() {
    if (status_.state != "scouting") return;
    auto sent = pt::call<ScoutReport>(*scout_, "report", ScoutStart{}, [this](paglets::Result<ScoutReport> r) {
        if (!r) return fail(pt::error_text("log scout", r.error()));
        if (r->state != "done") {
            (void)paglets::after_raw(100, "poll", {});
            return;
        }
        group(*r);
        // The scout's work is done.
        (void)pt::call<ScoutReport>(*scout_, "dispose", ScoutStart{}, [](paglets::Result<ScoutReport>) {});
        scout_.reset();
        if (status_.groups.empty()) return done();
        status_.state = "explaining";
        if (auto moved = paglets::dispatch(params_.via); !moved) fail(pt::error_text("dispatch", moved.error()));
    });
    if (!sent) fail(pt::error_text("log scout", sent.error()));
}

void group(const ScoutReport& r) {
    std::map<std::string, Group> by_shape;
    for (const auto& e : r.excerpts) {
        ++status_.lines;
        auto& g = by_shape[shape_of(e.line)];
        if (g.count++ == 0) {
            g.shape = shape_of(e.line);
            g.example = e.line;
        }
        if (std::ranges::find(g.hosts, e.host_name) == g.hosts.end()) g.hosts.push_back(e.host_name);
    }
    for (auto& [shape, g] : by_shape) {
        std::ranges::sort(g.hosts);
        status_.groups.push_back(std::move(g));
    }
    std::ranges::stable_sort(status_.groups, [](const Group& a, const Group& b) { return a.count > b.count; });
    if (static_cast<std::int64_t>(status_.groups.size()) > params_.max_groups) {
        status_.groups.resize(static_cast<std::size_t>(std::max<std::int64_t>(0, params_.max_groups)));
    }
}
```
<!-- /snippet -->

On the AI host, it classifies each group's example line with
`ai.classify` into one of the labels, and asks `ai.generate` for the
likely cause in one or two sentences, with a system prompt for log
messages. Then it goes home:

<!-- snippet: cpp/examples/explainer/explainer.cpp#explain -->
Source: [`cpp/examples/explainer/explainer.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/explainer/explainer.cpp)

```cpp
void explain(std::size_t i) {
    if (i >= status_.groups.size()) {
        status_.state = "returning";
        if (here_ == home_) return done();
        if (auto moved = paglets::dispatch(home_); !moved) fail(pt::error_text("dispatch home", moved.error()));
        return;
    }
    pt::lookup("ai", [this, i](paglets::Result<paglets::Endpoint> ai) {
        if (!ai) return fail(pt::error_text("ai", ai.error()));
        auto client = std::make_shared<svc::ai::Client>(*ai);
        const auto& g = status_.groups[i];
        auto sent = client->classify(
            svc::ai::ClassifyRequest{g.example, params_.labels, {}},
            [this, i, client](paglets::Result<svc::ai::ClassifyReply> c, Message&) {
                if (!c) return fail(pt::error_text("classify", c.error()));
                status_.groups[i].label = c->label;
                const auto& g = status_.groups[i];
                svc::ai::GenerateRequest q;
                q.system = "You explain log messages to system administrators.";
                q.prompt = "This log line appeared " + std::to_string(g.count) + " times on " +
                           std::to_string(g.hosts.size()) + " host(s): " + g.example +
                           "\nExplain its likely cause in one or two sentences.";
                auto asked = client->generate(q, [this, i](paglets::Result<svc::ai::TextReply> t, Message&) {
                    if (!t) return fail(pt::error_text("generate", t.error()));
                    status_.groups[i].explanation = t->text;
                    status_.model = t->model;
                    explain(i + 1);
                }, paglets::patterns::long_request());
                if (!asked) fail(pt::error_text("generate", asked.error()));
            }, paglets::patterns::long_request());
        if (!sent) fail(pt::error_text("classify", sent.error()));
    });
}
```
<!-- /snippet -->

At home, its state becomes `explained`, and it sends its owner a
notification: the number of distinct problems and log lines as the
title, and each group's label, count, example and explanation as the
text.

The messages and the status. `ScoutStart`, `ScoutStarted`,
`ScoutExcerpt` and `ScoutReport` are the fields of the Log Scout's
messages that the explainer uses; the encodings are maps by field name,
so the other fields keep their defaults:

<!-- snippet: cpp/examples/explainer/explainer_msgs.hpp -->
Source: [`cpp/examples/explainer/explainer_msgs.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/explainer/explainer_msgs.hpp)

```cpp
// Messages of the Log Explainer demo (planning/cpp-demo-paglets.md, demo 19).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace explainer_msgs {

// `start`: let a Log Scout (a child from `scout_module`, hex hash) search
// the logs, group the lines it found, explain each group on a host that
// offers `ai`, and come home with the explanations.
struct Explain {
    std::vector<std::string> hosts;  // where to search; empty: every host
    std::string root;
    std::string files = "**/*.log";
    std::vector<std::string> patterns = {"error", "fail"};
    std::int64_t window_ms = 3'600'000;
    std::string scout_module;
    std::string via = "offer:ai.generate";
    std::vector<std::string> labels = {"disk", "network", "permission", "configuration", "memory", "other"};
    std::int64_t max_groups = 10;
};

struct Started {
    bool accepted = false;
    std::string reason;
};

// A group of similar lines.
struct Group {
    std::string shape;  // the line with numbers, IDs and quoted text replaced by #
    std::string example;
    std::int64_t count = 0;
    std::vector<std::string> hosts;
    std::string label;  // ai.classify, one of the labels
    std::string explanation;  // ai.generate
};

struct Status {
    std::string state;  // idle, scouting, explaining, returning, explained, failed
    std::int64_t lines = 0;
    std::vector<Group> groups;  // the largest first
    std::vector<std::string> hosts;  // names of the hosts it was on, in order
    std::string ai_host;
    std::string model;
    std::string error;
};

// The Log Scout's messages (log_scout_msgs, the fields used here; the
// encodings are maps by field name, so the others keep their defaults).
struct ScoutStart {
    std::vector<std::string> hosts;
    std::string root;
    std::string files;
    std::vector<std::string> patterns;
    std::int64_t window_ms = 0;
    std::int64_t max_excerpts = 0;
};

struct ScoutStarted {
    bool accepted = false;
    std::string reason;
};

struct ScoutExcerpt {
    std::string host_name;
    std::string path;
    std::string line;
};

struct ScoutReport {
    std::string state;
    std::vector<ScoutExcerpt> excerpts;
    std::vector<std::string> errors;
};

}  // namespace explainer_msgs
```
<!-- /snippet -->

## Running it on a live mesh

The mesh needs hosts with a root of logs, and a host that offers `ai`:

```bash
H=build/macos-arm64/host/paglets-host
$H serve --key web1.key --ledger ledger-web1 --state state-web1 --root logs=/var/log/myapp \
    --join https://lab-1:7443 &
$H serve --key mac.key --ledger ledger-mac --state state-mac --ai ollama --ai-model llama3.2 \
    --join https://lab-1:7443 &
```

An admin adds the rules: read `logs`, and classify and generate with
`ai`:

```bash
$H ledger rule --ledger ledger --admin alice.key --name "read logs" --decision allow \
    --service files --op read --root logs
$H ledger rule --ledger ledger --admin alice.key --name explain --decision allow \
    --service ai --op classify --op generate
$H remote push --connect https://lab-1:7443 --key alice.key --ledger ledger
```

The scout is created on the explainer's home host, so its module must be
there. `remote push-module` stores it and prints its hash:

```bash
R="--connect https://lab-1:7443 --key olga.key --ledger ledger"
SCOUT=$($H remote push-module $R build/macos-arm64/guests/log_scout.wasm)
ID=$($H remote launch $R build/macos-arm64/guests/explainer.wasm)
$H remote call $R $ID start '{"hosts": ["web1", "web2"], "root": "logs", "files": "**/*.log",
    "patterns": ["error", "fail"], "window_ms": 3600000, "scout_module": "'$SCOUT'"}'
$H remote call $R $ID status --expect explained
```

`status` lists the groups with their shape, example, count, hosts, label
and explanation, the hosts the explainer was on, and the AI host.

## Test results

The test runs the explainer on a mesh of four hosts inside one test
process (`gateway: Log Explainer ...` in `cpp/tests/test_gateway.cpp`).
The hosts `web1` and `web2` keep logs with recent lines: four
`disk full` errors with different sizes, two refused connections to
different addresses, one `permission denied` for a quoted user, and an
`INFO` line that matches no pattern. `mac` offers `ai` through the `test`
backend, whose fixed answers pick the label that occurs most often in the
text, and repeat the system prompt and the prompt as the generated text.
The test checks that:

- the explainer goes from `home` to `mac` and back to `home`, and ends in
  the state `explained`;
- the scout found 7 lines, and they form 3 groups: 4 lines
  `error disk full on /data (# mb left)` from both hosts, labelled `disk`;
  2 lines `error network: connection to # refused`, labelled `network`;
  and `error permission denied for user #`, labelled `permission`;
- the explanation was generated with the system prompt;
- the owner has the notification `3 distinct problem(s) in 7 log line(s)`;
- afterwards only the explainer is left in the mesh: the scout and its
  clones are gone.

<!-- results: explainer -->
| Test | Mode | Outcome | Duration | Platform | Recorded | log lines found by the Log Scout | distinct problems after grouping | lines in the largest group |
|---|---|---|---|---|---|---|---|---|
| gateway: Log Explainer groups what a Log Scout found and explains it on the AI host | in-process | passed | 0.3 s | Darwin arm64, RelWithDebInfo build | 2026-10-16 | 7 | 3 | 4 |
| gateway: Log Explainer groups what a Log Scout found and explains it on the AI host | worker processes | passed | 0.4 s | Darwin arm64, RelWithDebInfo build | 2026-10-16 | 7 | 3 | 4 |
<!-- /results -->
