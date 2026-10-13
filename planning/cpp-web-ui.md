# paglets/cpp: the web UI `paglets-web` (WP31–WP37)

Status: in progress (started 2026-10-13). Decisions made while implementing
are listed in section 9; the docs pages (`docs/web-ui.md`,
`docs/interface-descriptions.md`) take precedence where they differ.

`paglets-web` is a local web UI for a mesh. It is a mesh client like
`paglets-host remote …`: it opens end-to-end sessions with hosts as an admin
or owner and serves a website on the loopback interface from its own C++
code (no web library). The welcome page shows the hosts of the mesh with
their properties and the number of active system and user paglets; a host
page shows its details and running paglets. Paglets that describe their
messages get forms to send them commands; modules that describe their start
parameters can be launched with a form; paglets can be dispatched, pinned
and disposed. Next to every view and action, the UI shows the
`paglets-host remote …` command that does the same in a shell.

## 1. Process and command line

```
paglets-web --connect URL --key KEY --ledger DIR [--host-key KEY-ID]
            [--passphrase-file FILE] [--port N] [--port-range A-B]
            [--no-port-search] [--listen ADDR] [--allow-remote]
            [--no-browser] [--module-dir DIR] [--refresh-ms N]
```

- The session flags are those of `paglets-host remote`.
- Port: `--port N` (explicit) or `--no-port-search` binds exactly that port
  or fails. Otherwise the default 8470 is tried, then the rest of the range
  (default 8470–8499), then an OS-chosen port. Ports are probed by binding
  (no check-then-bind race).
- `--listen` defaults to `127.0.0.1`; other addresses need `--allow-remote`.
- The browser is opened with `open` (macOS), `xdg-open` (Linux) or
  `ShellExecuteW` (Windows) unless `--no-browser`; the URL is printed.

## 2. HTTP server (`paglets_webui`)

Own minimal HTTP/1.1 server modelled on ck-git-hosting's dashboard server,
POSIX and Winsock: an accept thread polling 250 ms (stop flag), a worker pool
with a bounded queue, strict request parsing (16 KB headers, exactly one
Host, Content-Length bodies up to 1 MB, no chunked encoding),
`Connection: close`, security headers (CSP `default-src 'none'; script-src
'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:`,
`nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`,
`Cache-Control: no-store`).

Access control (the UI acts with the admin's or owner's key):

1. A random token per start, in the opened URL (`/?t=…`); the first request
   exchanges it for an `HttpOnly; SameSite=Strict` cookie. Every `/api` route
   needs the cookie (or `Authorization: Bearer TOKEN` for curl).
2. The Host header must name the bound address and port (`127.0.0.1:P`,
   `localhost:P`, `[::1]:P`): no DNS rebinding.
3. Requests that change something (POST) need a same-origin `Origin` header
   when one is sent.

Assets (one HTML shell, CSS, JS) are raw string literals compiled in.

## 3. Client library (`paglets_client`)

The session vocabulary moves out of `mesh_cli.cpp` into
`host/src/client/` (`paglets/client/client.hpp`): key and passphrase
handling, `Session::open`, `ask`, and typed requests returning plain structs
(status, hosts, landscape, modules, launch, call, send, describe, dispose,
dispatch, locate, pin, pins, unpin, push-module). `paglets-host remote` and
`paglets-web` both use it. `cli_equivalent` renders the shell command for an
action.

## 4. Protocol additions (additive; old hosts keep working)

| Request | Change |
|---|---|
| `status` | per paglet a fifth element `trust`; new `system` (names of native system paglets), `uptime_s`, `version`, `os`, `os_version`, `cpu_model`, `cpus`, `memory_total` |
| `send` (new) | one-way message: `paglet`, `name`, `payload`, `priority` |
| `describe` (new) | `paglet` or `module` (hash) → `interface` (JSON text) of the module, or of a system paglet's service |
| `modules` | per module `interface` (JSON text) when the module carries one |
| `terminate` (new) | admins: forced end of a paglet (`Runtime::terminate`) |

System paglets are paglets of trust class `system` (native `system.*` and
Wasm modules the ledger trusts as system); user paglets are the others.

## 5. Interface descriptions

A module may carry a custom section `paglets.interface` holding JSON:

```json
{
  "interface": 1,
  "name": "counter",
  "summary": "A counter that keeps a history",
  "launch": {"type": "none"},
  "messages": [
    {"name": "increment", "kind": "call", "summary": "Adds to the counter",
     "request": "Increment", "reply": "Status"},
    {"name": "status", "kind": "call", "request": "none", "reply": "Status"}
  ],
  "types": [ … as in schema.json … ]
}
```

- Types are the schema type names (`bool`, `i8`…`u64`, `f32`, `f64`,
  `string`, `bytes`, `list<T>`, `optional<T>`, record and enum names of
  `types`), plus `none` (empty payload) and `any` (free JSON).
- Message fields: `name`, `kind` (`call`: request with reply, `send`:
  one-way, `both`), `summary`, `request`, `reply`, `danger` (UI asks before
  sending), `example` (JSON).
- Field docs: records in `types` may give a field `doc` and `default`.
- The section is covered by the module hash (and so by signatures), and the
  host reads it without running the paglet.
- Build: `paglets_add_module(NAME … INTERFACE file.json)`. The interface
  tool (`paglets-interface`, compiled from `tools/interface`) merges the
  module's schema types into the description, validates it and appends the
  section after linking. Contract guests without an INTERFACE file get one
  generated from their services.
- `paglets-host module inspect` prints it; `remote describe` fetches it;
  the host answers `describe` for system paglets from their service
  descriptors.

## 6. Web pages

- `/` overview: mesh, entry host, role; host table (name, key, labels,
  state, OS/arch, CPUs, load, memory, slots, system and user paglets);
  totals.
- `/#host/KEY`: properties, links, system paglets, user paglets with
  actions (command, dispatch, pin, dispose, terminate), launch here.
- `/#paglet/HOST/ID`: interface with a form per message (call or send),
  replies, a log of this session's calls, actions.
- `/#modules`: modules of the hosts and of `--module-dir`, launch form from
  the `launch` description, push module, upload from the browser.
- `/#zombies`: zombie groups with state, sink and replicas; check,
  cancel, redirect.
- Every panel has a "Shell" box with the equivalent command; every action
  result shows the command it corresponds to.

JSON API (`/api/…`, documented in docs/web-ui.md): `GET mesh`,
`GET host?key=`, `GET paglet?host=&id=`, `GET modules`, `GET zombies`,
`POST call|send|launch|push-module|dispose|terminate|dispatch|pin|locate|
zombie-check|zombie-cancel|zombie-redirect`.

## 7. Tests

Unit tests: HTTP request parsing and limits, Host/Origin/token checks, port
search, interface parse/validate/embed/extract round trip, form-relevant
type parsing, `cli_equivalent`. End-to-end: a two-host mesh, `paglets-web
--no-browser --port 0`, mesh/host JSON, launch with arguments, call, send,
dispose; refusal without the token and with a foreign Origin. The existing
`remote` CLI tests keep passing.

## 8. Work packages

| WP | Content | Status |
|---|---|---|
| WP31 | `paglets_client` library extracted from `mesh_cli.cpp` (sessions, typed requests, `cli_equivalent`); `remote` commands on it; no behaviour change | done |
| WP32 | Protocol additions: `status` fields (trust, system list, host facts), `send`, `terminate`, `describe`, `modules` interfaces; CLI `remote send`, `remote terminate`, `remote describe`; tests | done |
| WP33 | Interface descriptions: format, parser/validator, custom section embed/extract (`paglets/wasm/interface.hpp`), `paglets-interface` tool, `paglets_add_module(INTERFACE …)`, generated descriptions for contract guests, `module inspect`; descriptions for the examples (counter, pi, hello, ping_pong, inventory, chronicle, calc_user); install for out-of-tree builds | done |
| WP34 | `paglets_webui` HTTP server: sockets (POSIX, Winsock), parser, limits, token/cookie, Host/Origin checks, port search, browser opening; unit tests | done |
| WP35 | `paglets-web` executable: mesh poller, JSON API, embedded UI (overview, host, paglet, modules and zombies pages, forms from interfaces, module upload, shell hints), actions | done |
| WP36 | End-to-end test (`web:` in `paglets_tests`: two hosts over HTTPS, the UI's API over HTTP), gateway User-Agent renamed to `paglets-gateway/1` | done |
| WP37 | Documentation (`docs/web-ui.md`, `docs/interface-descriptions.md`, CLI reference, READMEs, nav), docs sync; live check on the mesh hosts | open |

## 9. Decisions made while implementing

- **The interface is a post-link custom section.** clang places data with
  `__attribute__((section(".custom_section.…")))` into linear memory for
  wasm32-wasip1 reactors, so a small host tool appends the section instead.
- **`status` lists system paglets by ID** (`system.mesh-info`, …), so a
  tool can `describe` them directly.
- **`{"$bytes": "hex"}` in JSON payloads** becomes a MessagePack binary
  string (`bytes` fields), in `paglets-host remote` and in the web UI alike.
- **`remote launch` takes a module hash** as well as a file: the host must
  have the module (the web UI launches modules the hosts already store).
- **Pages are patched in place** on every refresh (a small DOM morph), so
  clicks and open details survive; the model's version only changes with
  its content.
- **The counter example takes start arguments** (`{"value": N}`), so the
  launch form has an example with parameters.
- **The end-to-end test runs in-process** (`tests/test_web.cpp`): two
  hosts over HTTPS that answer sessions as `paglets-host serve` does, the
  App and its server, and an HTTP client; no extra processes or tools.
- **The host answers `describe`**, from the module bytes, so the guest needs
  no code for it and C guests can carry descriptions too.
