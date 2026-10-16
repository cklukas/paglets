# Web Researcher

The Web Researcher answers a question with sources from the web, for a
host that has no internet access. It moves to a host that may search and
read the web, searches there and reads the first pages it finds, moves on
to a host with a language model that summarizes each page, and comes home
with a report of sources and summaries.

Directory: [`cpp/examples/researcher/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/researcher)

## What it shows

- **One paglet, two gateway hosts.** The web and the model are often on
  different machines. The researcher uses both: it goes to a host that
  offers `web.search` with the transfer ticket `offer:web.search`, and
  then to a host that offers `ai.summarize` with `offer:ai.summarize`.
  Hosts announce what their system paglets can do as offers (see
  [Service offers](../system-paglets.md#service-offers)).
- **Mediated web access.** It searches with `web.search` and reads pages
  with `web.extract_text`, which returns the readable text of a page. The
  `web` system paglet checks every URL against the mesh policy (see
  [Gateways to the web and to AI](../system-paglets.md#gateways-to-the-web-and-to-ai)).
- **Only the result comes home.** The page texts travel in the
  researcher's memory from the web host to the AI host. It drops them
  there, so only the report reaches the home host.
- **Home with what it has.** When a step fails away from home, it comes
  home with the sources it has and the reason.

## Why it is useful

Machines in protected networks have no browser on the internet, and
people there still need answers from the web. The researcher keeps the
internet access on one host, where the policy decides what may be
searched and read, and keeps the model on another. The person at the
protected host asks a question and gets back a short report with the
sources to cite, without either gateway knowing about the other.

## How it works

The researcher starts with a `start` message: the question, the number of
pages to read, the most words per summary, and the transfer tickets to
the web host and the AI host. It replies at once with its report in the
state `searching`, notes the key of its home host, and moves to the web
host.

There it searches for the question with `web.search` and keeps the title
and URL of up to `pages` results. Then it reads every result with
`web.extract_text`. A page without a title in the search results takes
the title of the page. A page that cannot be read gets a note instead of
a summary, and the others are still read. After the last page, it moves
to the AI host:

<!-- snippet: cpp/examples/researcher/researcher.cpp#search -->
Source: [`cpp/examples/researcher/researcher.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/researcher/researcher.cpp)

```cpp
// At the web host.
void search() {
    pt::lookup("web", [this](paglets::Result<paglets::Endpoint> web) {
        if (!web) return fail(pt::error_text("web", web.error()));
        web_ = *web;
        svc::web::Client c{web_};
        auto sent = c.search(svc::web::SearchRequest{params_.question, params_.pages},
                             [this](paglets::Result<svc::web::SearchReply> r, Message&) {
                                 if (!r) return fail(pt::error_text("search", r.error()));
                                 for (const auto& h : r->results) report_.sources.push_back(Source{h.title, h.url, {}});
                                 texts_.assign(report_.sources.size(), {});
                                 read(0);
                             });
        if (!sent) fail(pt::error_text("search", sent.error()));
    });
}

void read(std::size_t i) {
    if (i >= report_.sources.size()) {
        report_.state = "summarizing";
        return go(params_.ai);
    }
    svc::web::Client c{web_};
    auto sent = c.extract_text(svc::web::ExtractRequest{report_.sources[i].url},
                               [this, i](paglets::Result<svc::web::ExtractReply> r, Message&) {
                                   if (r) {
                                       texts_[i] = r->text;
                                       if (report_.sources[i].title.empty()) report_.sources[i].title = r->title;
                                   } else {
                                       report_.sources[i].summary = pt::error_text("(not read", r.error()) + ")";
                                   }
                                   read(i + 1);
                               });
    if (!sent) read(i + 1);
}
```
<!-- /snippet -->

On the AI host, it summarizes each page's text with `ai.summarize`, in at
most `max_words` words. A failed summary is noted in place of the
summary. Then it drops the texts and goes home:

<!-- snippet: cpp/examples/researcher/researcher.cpp#summarize -->
Source: [`cpp/examples/researcher/researcher.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/researcher/researcher.cpp)

```cpp
// At the AI host.
void summarize(std::size_t i) {
    if (i >= report_.sources.size()) {
        texts_.clear();
        report_.state = "returning";
        return go(home_);
    }
    if (texts_[i].empty()) return summarize(i + 1);
    pt::lookup("ai", [this, i](paglets::Result<paglets::Endpoint> ai) {
        if (!ai) return fail(pt::error_text("ai", ai.error()));
        svc::ai::Client c{*ai};
        auto sent = c.summarize(svc::ai::SummarizeRequest{texts_[i], params_.max_words, ""},
                                [this, i](paglets::Result<svc::ai::TextReply> r, Message&) {
                                    report_.sources[i].summary = r ? r->text : pt::error_text("(summarize", r.error()) + ")";
                                    summarize(i + 1);
                                });
        if (!sent) fail(pt::error_text("summarize", sent.error()));
    });
}
```
<!-- /snippet -->

At home, its state becomes `done`, and it tells its owner through the
`user-info` system paglet how many sources it found.

When a step fails, for example because there is no host to go to or the
search is refused, and the researcher is away from home, it goes home with
the report. On arrival, the state is `failed` and `error` gives the
reason. If even the move home fails, it stays where it is with the report:

<!-- snippet: cpp/examples/researcher/researcher.cpp#fail -->
Source: [`cpp/examples/researcher/researcher.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/researcher/researcher.cpp)

```cpp
void fail(std::string why) {
    report_.error = std::move(why);
    report_.state = "failed";
    if (here_ != home_ && !home_.empty()) {
        report_.state = "returning";  // home with what it has
        report_.error = "failed: " + report_.error;
        go(home_);
    }
}
```
<!-- /snippet -->

The messages and the report:

<!-- snippet: cpp/examples/researcher/researcher_msgs.hpp -->
Source: [`cpp/examples/researcher/researcher_msgs.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/researcher/researcher_msgs.hpp)

```cpp
// Messages of the Web Researcher demo (planning/cpp-demo-paglets.md, demo 21).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace researcher_msgs {

// `start`: search the web for `question` on a host offering `web.search`,
// read the first `pages` results there, summarize them on a host offering
// `ai.summarize`, and come home with the report.
struct Question {
    std::string question;
    std::int64_t pages = 3;
    std::int64_t max_words = 60;
    std::string web = "offer:web.search";
    std::string ai = "offer:ai.summarize";
};

struct Source {
    std::string title;
    std::string url;
    std::string summary;
};

struct Report {
    std::string state;  // idle, searching, summarizing, returning, done, failed
    std::string question;
    std::vector<Source> sources;
    std::vector<std::string> hosts;  // where it went
    std::string error;
};

}  // namespace researcher_msgs
```
<!-- /snippet -->

## Running it on a live mesh

The mesh needs a host that offers `web` with a search engine, and a host
that offers `ai`. `--web-search` names a SearXNG instance, or another
endpoint that answers in SearXNG's JSON format:

```bash
H=build/macos-arm64/host/paglets-host
$H serve --key gw.key --ledger ledger-gw --state state-gw --web \
    --web-search "https://searx.example.org/search?q={query}&format=json" --join https://lab-1:7443 &
$H serve --key mac.key --ledger ledger-mac --state state-mac --ai ollama --ai-model llama3.2 \
    --join https://lab-1:7443 &
```

An admin adds the rules: search and read pages with `web`, and summarize
with `ai`. For the service `web`, `--root` and `--path` would limit the
host names and paths of the URLs:

```bash
$H ledger rule --ledger ledger --admin alice.key --name "web research" --decision allow \
    --service web --op search --op extract_text
$H ledger rule --ledger ledger --admin alice.key --name summaries --decision allow \
    --service ai --op summarize
$H remote push --connect https://lab-1:7443 --key alice.key --ledger ledger
```

Then the owner launches the researcher on a host without internet access
and asks the question. It comes home, so the home host answers for the
report:

```bash
R="--connect https://lab-1:7443 --key olga.key --ledger ledger"
ID=$($H remote launch $R build/macos-arm64/guests/researcher.wasm)
$H remote call $R $ID start '{"question": "how to tune ZFS for databases", "pages": 3, "max_words": 60}'
$H remote call $R $ID report
```

`report` lists every source with its title, URL and summary, and the
hosts the researcher was on.

## Test results

The test runs the researcher on a mesh of three hosts inside one test
process (`gateway: Web Researcher ...` in `cpp/tests/test_gateway.cpp`).
`gateway` offers `web`, limited to a small web server of the test on the
local machine, which also answers searches in SearXNG's format with two
of its pages. `mac` offers `ai` through the `test` backend, whose fixed
summary is the first `max_words` words of the text, followed by `...`
when there are more. The researcher asks about "quarterly numbers", with
two pages and three words per summary. The test checks that:

- the researcher goes from `home` to `gateway`, `mac` and back to `home`,
  and ends in the state `done`;
- the report has two sources; the first has the title from the search
  results, `The report`, and the summary `quarterly numbers: 42`, the
  whole text of the page;
- the second, an HTML page, has the summary `Hello First bold ...`: the
  first words of its readable text, without the markup.

<!-- results: researcher -->
| Test | Mode | Outcome | Duration | Platform | Recorded | hosts on the way | pages read and summarized |
|---|---|---|---|---|---|---|---|
| gateway: Web Researcher searches and reads on the web host, summarizes on the AI host, comes home | in-process | passed | 0.1 s | Darwin arm64, RelWithDebInfo build | 2026-10-16 | 4 | 2 |
| gateway: Web Researcher searches and reads on the web host, summarizes on the AI host, comes home | worker processes | passed | 0.3 s | Darwin arm64, RelWithDebInfo build | 2026-10-16 | 4 | 2 |
<!-- /results -->
