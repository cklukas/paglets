# AI Document Digest

The AI Document Digest collects text documents from several hosts, has
each one summarized by a language model on a host that offers `ai`, and
writes a digest of all summaries to a file on another host. The hosts that
keep the documents need no model, and the host with the model needs no
access to the documents' directories.

Directory: [`cpp/examples/digest/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/digest)


File selection uses the framework's background scan-task helper. The agent
stays responsive while the directory walk runs; bounded list results and
operation deadlines are separate from handler time. See
[Long-running agents and tasks](../long-running-tasks.md) for the shared
`TaskHandle` API, cancellation, progress and the example audit.

## What it shows

- **A paglet that goes to the AI.** The digest reads the documents on
  their hosts, then moves with them in its memory to a host that offers
  `ai.summarize`. It finds that host with the transfer ticket
  `offer:ai.summarize`: hosts announce what their system paglets can do as
  offers, and `mesh-info` spreads them through the mesh (see
  [Service offers](../system-paglets.md#service-offers)).
- **Grants on three hosts.** It asks the `grants` system paglet of each
  source host for `read` on a named root, and the output host for `write`
  and `create`. The mesh policy decides; the AI host checks a rule for the
  service `ai` as well (see
  [Gateways to the web and to AI](../system-paglets.md#gateways-to-the-web-and-to-ai)).
- **Data residency.** A paglet that reads from a root carries that root's
  mark. If a residency rule keeps the root's content on its host, the move
  to the AI host is refused, and the digest stays where it read the
  content (see [Data residency](../hosts-and-meshes.md#data-residency)).
  Confidential text never reaches the model.

## Why it is useful

Documents often live on the machines where they are written: reports on a
Linux server, budgets on a Windows desktop. A language model runs where
there is a GPU. The digest brings the text to the model for a moment,
leaves only the summaries in the result, and writes them where people
read them. Nothing is installed on the document hosts, and the policy
decides which roots may be read, which model may be used and which content
may not leave its host at all.

## How it works

The digest starts with a `start` message that lists the sources (a host,
a named root and a file pattern each), the output file, the transfer
ticket to the AI host, and limits. It asks `mesh-info` which host it is
on, and then works through its plan: the state in its memory says what to
do on each arrival. A refused move ends in `refused` when data residency
was the reason:

<!-- snippet: cpp/examples/digest/digest.cpp#states -->
Source: [`cpp/examples/digest/digest.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/digest/digest.cpp)

```cpp
void on_arrived(const abi::ArrivedEvent&) override {
    where([this] {
        if (status_.state == "collecting") return collect();
        if (status_.state == "summarizing") return summarize(0);
        if (status_.state == "writing") return write();
    });
}

void on_move_failed(const abi::MoveFailed& f) override {
    // Refused by data residency, or no host to go to: it stays here.
    status_.state = f.reason.find("data residency") != std::string::npos ? "refused" : "failed";
    status_.error = f.reason;
    paglets::log("digest: " + f.reason);
}
```
<!-- /snippet -->

On each source host, it gets a directory with `read`, finds the matching
files of at most 64 KB (the framework background scan-task helper), and reads them into its memory,
until it holds `max_files` documents:

<!-- snippet: cpp/examples/digest/digest.cpp#read -->
Source: [`cpp/examples/digest/digest.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/digest/digest.cpp)

```cpp
void read_next(std::size_t i) {
    if (i >= found_.size() || static_cast<std::int64_t>(texts_.size()) >= params_.max_files) {
        dir_.reset();
        ++source_;
        return next_source();
    }
    svc::files::Client client{files_};
    auto sent = client.read(
        svc::files::ReadRequest{found_[i], 0, max_document},
        [this, i](paglets::Result<svc::files::ReadReply> r, Message&) {
            if (r) {
                Document d;
                d.host = here_name_;
                d.path = params_.sources[source_].root + "/" + found_[i];
                d.size = static_cast<std::int64_t>(r->size);
                status_.documents.push_back(std::move(d));
                texts_.emplace_back(r->data.begin(), r->data.end());
            }
            read_next(i + 1);
        },
        lending(*dir_));
    if (!sent) fail(error_text("read", sent.error()));
}
```
<!-- /snippet -->

After the last source, it moves to the AI host. There it summarizes the
documents one by one with `ai.summarize`, in at most `max_words` words,
and notes the model that answered. Then it drops the texts, so only the
summaries travel on to the output host:

<!-- snippet: cpp/examples/digest/digest.cpp#summarize -->
Source: [`cpp/examples/digest/digest.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/digest/digest.cpp)

```cpp
void summarize(std::size_t i) {
    if (i >= texts_.size()) {
        texts_.clear();
        status_.state = "writing";
        if (is_here(params_.output.host)) return write();
        return go(params_.output.host);
    }
    lookup("ai", [this, i](paglets::Result<Endpoint> ai) {
        if (!ai) return fail(error_text("ai", ai.error()));
        svc::ai::Client client{*ai};
        svc::ai::SummarizeRequest q;
        q.text = texts_[i];
        q.max_words = params_.max_words;
        auto sent = client.summarize(q, [this, i](paglets::Result<svc::ai::TextReply> r, Message&) {
            if (!r) return fail(error_text("summarize", r.error()));
            status_.documents[i].summary = r->text;
            status_.model = r->model;
            summarize(i + 1);
        }, paglets::patterns::long_request());
        if (!sent) fail(error_text("summarize", sent.error()));
    });
}
```
<!-- /snippet -->

On the output host, it asks for the output root with `write` and
`create`, and writes the digest as Markdown: a heading, the model, and one
section per document (`## host: root/path`) with its summary. Its state is
then `written`, and it stays on the output host.

The messages and the status:

<!-- snippet: cpp/examples/digest/digest_msgs.hpp -->
Source: [`cpp/examples/digest/digest_msgs.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/digest/digest_msgs.hpp)

```cpp
// Messages of the AI Document Digest demo (planning/cpp-web-ai.md, section 5).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace digest_msgs {

// Where to look for documents: a host (name or key ID), a named root on it
// and a file pattern.
struct Source {
    std::string host;
    std::string root;
    std::string pattern = "**/*.txt";
};

// Where the digest goes: a file below a named root of a host.
struct Output {
    std::string host;
    std::string root;
    std::string path = "digest.md";
};

// `start`: collect the documents, summarize them where `ai` is offered,
// write the digest.
struct Start {
    std::vector<Source> sources;
    Output output;
    std::string ai = "offer:ai.summarize";  // the transfer ticket to the AI host
    std::int64_t max_files = 20;
    std::int64_t max_words = 60;
};

struct Started {
    bool accepted = false;
    std::string reason;
};

struct Document {
    std::string host;
    std::string path;
    std::int64_t size = 0;
    std::string summary;
};

struct Status {
    std::string state;  // idle, collecting, summarizing, writing, written, refused, failed
    std::vector<Document> documents;
    std::vector<std::string> hosts;  // names of the hosts it was on, in order
    std::string model;
    std::string error;
};

}  // namespace digest_msgs
```
<!-- /snippet -->

## Running it on a live mesh

The mesh needs hosts with document roots, a host with an output root, and
a host that offers `ai`, for example through Ollama:

```bash
H=build/macos-arm64/host/paglets-host
$H serve --key linux.key --ledger ledger-linux --state state-linux --root docs=/srv/docs \
    --join https://lab-1:7443 &
$H serve --key windows.key --ledger ledger-windows --state state-windows --root docs=C:/docs \
    --root secret=C:/board --join https://lab-1:7443 &
$H serve --key archive.key --ledger ledger-archive --state state-archive --root out=/srv/digests \
    --join https://lab-1:7443 &
$H serve --key mac.key --ledger ledger-mac --state state-mac --ai ollama --ai-model llama3.2 \
    --join https://lab-1:7443 &
```

An admin adds the rules: read `docs` and `secret`, write and create in
`out`, and summarize with `ai`. A residency rule keeps the confidential root `secret`
on its host. The rules apply to every paglet; `--owner` or `--group` narrow them
(see [Policy](../hosts-and-meshes.md#policy)):

```bash
$H ledger rule --ledger ledger --admin alice.key --name "read documents" --decision allow \
    --service files --op read --root docs --root secret
$H ledger rule --ledger ledger --admin alice.key --name "write digests" --decision allow \
    --service files --op write --op create --root out
$H ledger rule --ledger ledger --admin alice.key --name summaries --decision allow \
    --service ai --op summarize
$H ledger residency --ledger ledger --admin alice.key secret host-only
$H remote push --connect https://lab-1:7443 --key alice.key --ledger ledger
```

Then the owner launches the digest and starts it:

```bash
R="--connect https://lab-1:7443 --key olga.key --ledger ledger"
ID=$($H remote launch $R build/macos-arm64/guests/digest.wasm)
$H remote call $R $ID start '{"sources": [{"host": "linux", "root": "docs", "pattern": "**/*.txt"},
    {"host": "windows", "root": "docs", "pattern": "**/*.txt"}],
    "output": {"host": "archive", "root": "out", "path": "digest.md"}, "max_words": 60}'
```

The digest ends on the output host, or stays on a source host when it was
refused. `remote call` reaches a paglet on the host where it is, so find
it first with `remote locate`, and ask that host for the status:

```bash
$H remote locate $R $ID
$H remote call --connect https://archive:7443 --key olga.key --ledger ledger $ID status
```

`state` is `written` when the digest is in place. `hosts` lists the hosts
the digest was on, and every document has its host, path, size and
summary.

## Test results

The test runs the digest on a mesh of four hosts inside one test process
(`gateway: AI Document Digest ...` in `cpp/tests/test_gateway.cpp`). The
`mac` host offers `ai` through the `test` backend, which gives fixed
answers: a summary is the first `max_words` words of the text, followed by
`...` when there are more. The test checks that:

- a digest of `**/*.txt` on the `docs` roots of `linux` and `windows`
  visits `linux`, `windows`, `mac` and `archive` in that order, and ends
  on `archive` in the state `written`;
- the two documents get the expected summaries, and a Markdown file that
  does not match the pattern is left out;
- the file `digest.md` on `archive` has a section
  `## linux: docs/reports/launch.txt`, the summaries, and the model's name;
- a digest of a root with the residency rule `host-only` is refused on its
  way to the AI host: its state is `refused`, the reason names data
  residency, and it stays on the `windows` host with the document it read.

<!-- results: digest -->
| Test | Mode | Outcome | Duration | Platform | Recorded | hosts on the way | documents summarized | host-only root: kept off the AI host |
|---|---|---|---|---|---|---|---|---|
| gateway: AI Document Digest reads on two hosts, summarizes on the AI host, writes on a third (WP17 exit) | in-process | passed | 20.2 s | Darwin arm64, RelWithDebInfo build | 2026-10-16 | 4 | 2 | yes |
| gateway: AI Document Digest reads on two hosts, summarizes on the AI host, writes on a third (WP17 exit) | worker processes | passed | 40.3 s | Darwin arm64, RelWithDebInfo build | 2026-10-16 | 4 | 2 | yes |
<!-- /results -->
