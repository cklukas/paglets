# paglets/cpp: service offers, `web` and `ai` (WP17)

Status: implemented, except the Apple Foundation Models backend (it needs a
macOS 27 machine to build and test its Swift bridge) and llama.cpp. Companion
to the plan (WP17) and to the security design, sections 6.6 to 6.8. Code:
offers in `cpp/host/src/node/compute.cpp` and `movement.cpp`, the gateway
system paglets in `cpp/host/src/gateway/`, contracts in
`cpp/common/include/paglets/services/web.hpp` and `ai.hpp`, the demos in
`cpp/examples/courier/` and `cpp/examples/digest/`; tests in
`cpp/tests/test_gateway.cpp`, `test_compute.cpp` (offers) and
`host_mesh_discovery`. Data residency has its own page:
[cpp-residency.md](cpp-residency.md).

## 1. Service offers

Hosts differ in what they can do. A system paglet that offers a feature
announces it as an **offer**: the service, its operations and typed
attributes (text, comma-separated lists, or numbers).

| Service | Attributes |
|---|---|
| `web` | `internet` (yes/no), `internal` (allowed URL prefixes), `max_bytes`, `max_download_bytes`, `queue` |
| `ai` | `backend`, `models`, `tasks`, `context`, `vision` |

- Offers travel in the host's mesh-info snapshot, so every host knows the
  offers of the whole mesh within seconds; there is no registry.
- Offers are dynamic: `Node::set_offer` replaces a service's offer,
  `withdraw_offer` removes it, and the snapshot goes out at once. The `ai`
  paglet probes its backend every 30 s and withdraws its offer while the
  backend does not answer.
- `mesh-info.find_offers` (`service`, `op`, `require`, `prefer`, `limit`):
  requirements are `=` (text), `has` (list contains), `>=` and `<=`
  (numbers); `prefer` orders by an attribute (`-name`: largest first),
  otherwise the least loaded host comes first.
- Transfer tickets name offers: `offer:ai.summarize` moves a paglet to the
  best host offering the operation, after the usual checks (ABI, manifest
  preflight, data residency).

## 2. `web`

On hosts started with `--web`. Paglets never get sockets: they move to a
web host and ask `web` (found through the directory).

| Operation | Does |
|---|---|
| `capabilities` | what the host reaches, limits, proxy, search |
| `fetch` | GET or HEAD; status, headers, body (bounded; `truncated`) |
| `extract_text` | title, readable text and absolute links of a page |
| `download` | the body as an artifact (an `artifact` capability in the reply); optional expected SHA-256 |
| `search` | a configured JSON search backend (SearXNG style) |

Rules:

- **Destinations**: every address the URL's host name resolves to is
  checked; loopback, private, link-local, shared, multicast, documentation
  and similar ranges (IPv4 and IPv6, also mapped, NAT64 and 6to4 forms) are
  refused unless the URL starts with a prefix the host's admin allowed
  (`--web-internal`). The connection goes to the address that was checked,
  so a second DNS answer cannot redirect it. Redirects are followed by hand
  and checked like the first request. With `--web-no-internet` only the
  allowed prefixes are reachable.
- **Policy**: the mesh decides per request. The item is service `web`, the
  operation, the URL's host as the root and its path (without the leading
  slash) as the path, so rules and grants can name sites and path patterns.
- **No uploads**: GET and HEAD only. No credentials in URLs.
- **Proxy**: the host's proxy (`--web-proxy`, default `HTTPS_PROXY`). Behind
  a proxy, names the host cannot resolve are left to the proxy.
- Requests run on up to four threads of their own; the paglet's request is
  answered when done. Downloaded artifacts carry the downloader's residency
  marks.

## 3. `ai`

On hosts started with `--ai ollama` (or `--ai test`). Content given to `ai`
never leaves the host for inference.

| Operation | Does |
|---|---|
| `capabilities` | backend, models, tasks, limits, queue |
| `summarize`, `classify`, `extract`, `generate` | text tasks (prompts for the backend's model) |
| `embed` | vectors for semantic search |
| `describe_image` | models with vision support |

- **Backends**: Ollama through its local HTTP API (admins choose the exposed
  models with `--ai-model`); `test` answers deterministically from the input
  (the first words as the summary, the most frequent label, `name: value`
  lines) for tests and demonstrations without a model.
- **Policy**: service `ai`, the operation, the model as the root.
- **Quotas**: requests per owner and hour (600 by default); inputs up to
  256 KB.
- Long requests are answered when done, on up to two threads.
- Apple Foundation Models (macOS 27, Apple silicon with Apple Intelligence)
  needs a small Swift bridge with a C interface, built only on macOS; it
  waits for such a machine.

## 4. Command line

`paglets-host serve --web [--web-internal URL-PREFIX]... [--web-no-internet]
[--web-proxy URL] [--web-search URL] [--web-ca FILE]` and `--ai ollama|test
[--ai-url URL] [--ai-model NAME]...`; `paglets-host remote landscape` lists
every host's offers.

## 5. Demos and exit

- **Download Courier** (`cpp/examples/courier/`): started on a host without
  internet access, the courier moves with the ticket `offer:web.download`,
  downloads the file into an artifact there, reads it into its memory, goes
  home, stores it as an artifact and notifies its owner. A failed errand
  (for example an intranet address) brings it home with the reason.
- **AI Document Digest** (`cpp/examples/digest/`): the digest visits the
  hosts with documents, asks `grants` for a directory of the named root
  (the policy decides), reads the matching files, moves with
  `offer:ai.summarize` to the AI host, summarizes them, and writes the
  digest on a third host. If it read a root whose content stays on its host,
  the move is refused and it stays there (state `refused`).

| Test | Covers |
|---|---|
| `test_gateway.cpp`: addresses, URLs, text | internal ranges, URL resolution, HTML text and links |
| web | fetch with redirects, bounded bodies, refusals (other internal addresses, a redirect into them, schemes, methods), text extraction, downloads with hashes |
| ai | the test backend's tasks, embeddings, unknown models, quotas, the offer |
| Download Courier (exit) | home, gateway (the only web host) and a third host; the file comes home as an artifact and the owner is notified; an intranet address is refused |
| AI Document Digest (exit) | documents on a "linux" and a "windows" host, summaries on the only AI host ("mac"), the digest written on a fourth host; a `host-only` root keeps the paglet where it read it |
| `test_compute.cpp`: offers | gossip, `find_offers` requirements and order, `offer:` tickets, withdrawal |
| `host_mesh_discovery` | a `serve --ai test` host's offer in `remote landscape` |

The demo hosts stand in for the operating systems of the plan's exit; the
real cross-platform run (a Windows host, a macOS 27 host with Apple
Foundation Models) needs that hardware.
