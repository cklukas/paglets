# Semantic Mesh Search

The Semantic Mesh Search finds documents by meaning rather than by words.
It reads text documents on the hosts that keep them, moves to a host that
offers `ai.embed`, and turns every document into a vector there. A
question is turned into a vector the same way, and the documents whose
vectors point in the most similar direction are the answer.

Directory: [`cpp/examples/semantic/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/semantic)

## What it shows

- **An index without a database.** The index is an ordinary
  `std::vector` in the paglet's memory: each entry holds the host, the
  path, the text and its vector. It is part of the paglet's memory image,
  so it survives deactivation and moves with the paglet.
- **A paglet that stays with the model.** After indexing, the paglet
  stays on the AI host and answers questions there. It reached that host
  with the transfer ticket `offer:ai.embed`; hosts announce what their
  system paglets can do as offers (see
  [Service offers](../system-paglets.md#service-offers)).
- **Grants per source host.** On each source host it asks the `grants`
  system paglet for `read` on a named root. The AI host checks a policy
  rule for the service `ai` (see
  [Gateways to the web and to AI](../system-paglets.md#gateways-to-the-web-and-to-ai)).
- **Data residency.** Reading a document marks the paglet with the root
  it came from. If a residency rule keeps that root's content on its host,
  the move to the AI host is refused and the search fails before any text
  leaves (see [Data residency](../hosts-and-meshes.md#data-residency)).

## Why it is useful

Notes, reports and manuals are spread over many machines, and a search
for words misses a document that says the same thing in other words.
Embedding models are small and fast, but they run where they are
installed. The search brings the text to the model once, keeps the
vectors, and from then on answers each question with a single embedding
request and a comparison in memory. Nothing needs to be installed on the
document hosts, and no search server needs to run.

## How it works

The search starts with an `index` message: a list of sources (a host, a
named root and a file pattern each), the transfer ticket to the AI host,
and the most files to take from each source. It replies at once with its
status, `collecting`. Like the [AI Document Digest](ai-digest.md), it
visits the source hosts one after another, gets a directory with `read`,
finds the matching files of at most 64 KB, and reads them into its
memory. Each document becomes an entry of the index:

<!-- snippet: cpp/examples/semantic/semantic.cpp#index -->
Source: [`cpp/examples/semantic/semantic.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/semantic/semantic.cpp)

```cpp
struct Doc {
    std::string host_name;
    std::string path;
    std::string text;
    std::vector<double> vector;
};
```
<!-- /snippet -->

After the last source, it moves to the AI host. There it sends the texts
to `ai.embed`, 16 at a time, and keeps each vector with its document. It
also keeps the endpoint of the `ai` system paglet and the name of the
model, so that questions use the same model:

<!-- snippet: cpp/examples/semantic/semantic.cpp#embed -->
Source: [`cpp/examples/semantic/semantic.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/semantic/semantic.cpp)

```cpp
// At the AI host: the documents' vectors, a few at a time.
void embed(std::size_t from) {
    if (from >= docs_.size()) {
        status_.state = "ready";
        status_.documents = static_cast<std::int64_t>(docs_.size());
        return;
    }
    pt::lookup("ai", [this, from](paglets::Result<paglets::Endpoint> ai) {
        if (!ai) return fail(pt::error_text("ai", ai.error()));
        ai_ = *ai;
        svc::ai::EmbedRequest q;
        const std::size_t to = std::min(docs_.size(), from + 16);
        for (std::size_t i = from; i < to; ++i) q.texts.push_back(docs_[i].text);
        svc::ai::Client c{ai_};
        auto sent = c.embed(q, [this, from, to](paglets::Result<svc::ai::EmbedReply> r, Message&) {
            if (!r || r->vectors.size() != to - from) return fail(r ? "embed: wrong count" : pt::error_text("embed", r.error()));
            status_.model = r->model;
            for (std::size_t i = from; i < to; ++i) docs_[i].vector = std::move(r->vectors[i - from].values);
            embed(to);
        });
        if (!sent) fail(pt::error_text("embed", sent.error()));
    });
}
```
<!-- /snippet -->

When every document has its vector, the state is `ready`. A `query`
message carries a question in plain language and the number of hits
wanted. The paglet embeds the question, compares its vector with every
document's by cosine similarity, and replies with the best hits, each
with its host, path, score and the first 120 bytes of the text. The reply
is deferred until the embedding arrives:

<!-- snippet: cpp/examples/semantic/semantic.cpp#query -->
Source: [`cpp/examples/semantic/semantic.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/semantic/semantic.cpp)

```cpp
void query(const Query& q, Message& m) {
    if (status_.state != "ready") {
        (void)m.reply(Hits{{}, "the index is not ready (" + status_.state + ")"});
        return;
    }
    auto reply = std::make_shared<paglets::Capability>(m.defer_reply());
    svc::ai::Client c{ai_};
    auto sent = c.embed(svc::ai::EmbedRequest{{q.text}, status_.model},
                        [this, q, reply](paglets::Result<svc::ai::EmbedReply> r, Message&) {
                            Hits hits;
                            if (!r || r->vectors.empty()) {
                                hits.error = r ? "no vector" : pt::error_text("embed", r.error());
                                (void)paglets::reply_to(*reply, hits);
                                return;
                            }
                            for (const auto& d : docs_) {
                                std::string excerpt = d.text.substr(0, 120);
                                hits.hits.push_back(Hit{d.host_name, d.path, cosine(r->vectors[0].values, d.vector),
                                                        std::move(excerpt)});
                            }
                            std::ranges::sort(hits.hits, [](const Hit& a, const Hit& b) { return a.score > b.score; });
                            hits.hits.resize(std::min<std::size_t>(
                                hits.hits.size(), static_cast<std::size_t>(std::max<std::int64_t>(1, q.limit))));
                            (void)paglets::reply_to(*reply, hits);
                        });
    if (!sent) (void)paglets::reply_to(*reply, Hits{{}, pt::error_text("embed", sent.error())});
}
```
<!-- /snippet -->

A `query` before the index is ready gets an empty list and an error that
names the state.

The messages and the status:

<!-- snippet: cpp/examples/semantic/semantic_msgs.hpp -->
Source: [`cpp/examples/semantic/semantic_msgs.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/semantic/semantic_msgs.hpp)

```cpp
// Messages of the Semantic Mesh Search demo (planning/cpp-demo-paglets.md,
// demo 17).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace semantic_msgs {

struct Source {
    std::string host;
    std::string root;
    std::string pattern = "**/*.txt";
};

// `index`: read the documents of `sources`, move to a host that offers
// `ai.embed`, and keep their embeddings in memory there.
struct Index {
    std::vector<Source> sources;
    std::string ai = "offer:ai.embed";
    std::int64_t max_files = 200;
};

// `query`: the documents closest in meaning to `text`.
struct Query {
    std::string text;
    std::int64_t limit = 3;
};

struct Hit {
    std::string host_name;
    std::string path;
    double score = 0;  // cosine similarity
    std::string excerpt;
};

struct Hits {
    std::vector<Hit> hits;
    std::string error;
};

struct Status {
    std::string state;  // idle, collecting, embedding, ready, failed
    std::int64_t documents = 0;
    std::string host_name;  // where the index is
    std::string model;
    std::string error;
};

}  // namespace semantic_msgs
```
<!-- /snippet -->

## Running it on a live mesh

The mesh needs hosts with document roots and a host that offers `ai` with
an embedding model, for example through Ollama:

```bash
H=build/macos-arm64/host/paglets-host
$H serve --key mac.key --ledger ledger-mac --state state-mac --ai ollama --ai-model nomic-embed-text \
    --join https://lab-1:7443 &
```

The paglet asks for embeddings without naming a model, so the AI host
uses its first model. Offer the embedding model first, or only it, as
above.

An admin adds the rules: read the root `docs`, and embed with `ai`. For
the service `ai`, `--root` would name a model:

```bash
$H ledger rule --ledger ledger --admin alice.key --name "read docs" --decision allow \
    --service files --op read --root docs
$H ledger rule --ledger ledger --admin alice.key --name embeddings --decision allow \
    --service ai --op embed
$H remote push --connect https://lab-1:7443 --key alice.key --ledger ledger
```

Then the owner launches the search and builds the index:

```bash
R="--connect https://lab-1:7443 --key olga.key --ledger ledger"
ID=$($H remote launch $R build/macos-arm64/guests/semantic.wasm)
$H remote call $R $ID index '{"sources": [{"host": "lab-1", "root": "docs", "pattern": "**/*.txt"},
    {"host": "lab-2", "root": "docs", "pattern": "**/*.md"}]}'
```

The index is built on the AI host, and `remote call` reaches a paglet on
the host where it is. So ask the AI host for the status and the queries:

```bash
M="--connect https://mac:7443 --key olga.key --ledger ledger"
$H remote call $M $ID status --expect ready
$H remote call $M $ID query '{"text": "how do we restore a backup?", "limit": 3}'
```

`remote locate` tells where the paglet is, if in doubt.

## Test results

The test runs the search on a mesh of three hosts inside one test process
(`gateway: Semantic Mesh Search ...` in `cpp/tests/test_gateway.cpp`).
Host `a` keeps a document about a rocket launch and one about soup, host
`b` one about a garden, and `mac` offers `ai` through the `test` backend.
Its fixed answers include embeddings that hash the words of a text into
64 dimensions, so texts with words in common get similar vectors. The test
checks that:

- the index is built on `mac`, reaches the state `ready`, and holds the
  three documents of both hosts;
- the question "rocket engines launch" finds `docs/rockets.txt` of host
  `a` first, with a higher score than the second hit;
- the question "water the tomatoes in the garden" finds `docs/garden.txt`
  of host `b` first.

<!-- results: semantic -->
| Test | Mode | Outcome | Duration | Platform | Recorded | documents indexed from two hosts | questions answered by meaning |
|---|---|---|---|---|---|---|---|
| gateway: Semantic Mesh Search indexes documents on the AI host and answers by meaning | in-process | passed | 0.2 s | Darwin arm64, Release build | 2026-10-12 | 3 | 2 |
| gateway: Semantic Mesh Search indexes documents on the AI host and answers by meaning | worker processes | passed | 0.3 s | Darwin arm64, Release build | 2026-10-12 | 3 | 2 |
<!-- /results -->
