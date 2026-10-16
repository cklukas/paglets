# Command-line reference

paglets/cpp builds these programs:

| Program | Purpose |
|---|---|
| `paglets-host` | runs paglets on one host, manages keys and the mesh ledger, runs a host of a mesh, and talks to live hosts (`build/<preset>/host/`) |
| `paglets-wamrc` | optional compiler for host-local compute-job AOT code; built when LLVM is available |
| `paglets-worker` | runs paglet instances for `paglets-host`, in an operating-system sandbox (`build/<preset>/host/`) |
| `paglets-web` | a local web UI for a mesh (`build/<preset>/web/`, see [The web UI](web-ui.md)) |
| `paglets-interface` | puts [interface descriptions](interface-descriptions.md) into modules; the build runs it (`build/<preset>/examples/`) |
| `paglets-spike` | the measurement and memory-image tool of milestones M0 and M1 (`build/<preset>/host/`) |

The examples use `H=build/linux-gcc16/host/paglets-host`.

## Conventions

- Options take their value as the next argument (`--state-dir DIR`, not
  `--state-dir=DIR`). Options marked "repeatable" may be given several times.
- An unknown option, a missing value or a missing argument prints the usage
  of the command group to stderr and exits with status 2.
- Other failures print `paglets-host: <reason>` and exit with status 1.
- Integer options in `paglets-host` commands and job cancellation IDs require the whole
  decimal integer: trailing text, fractions, whitespace and a leading `+`
  are refused. Durations and quorums are positive; rule priorities may be
  signed. Counts must fit their destination type; job IDs and compute slots
  are positive. Listen ports are 0–65535; beacon ports are 1–65535.
  `remote call --timeout` separately accepts decimal seconds.
- Message bodies and arguments are JSON. `paglets-host` converts them to
  MessagePack before they reach the paglet and prints replies as JSON (`ok` for an empty
  reply, `error: <code>` for an [error code](configuration.md#error-codes)).
  `{"$bytes": "00ff"}` is a binary string (for `bytes` fields); binary
  strings in replies print as arrays of numbers. A request without a
  handler answers `error: unknown_message`.
- `KEY` is a key file, `KEY-ID` a key's ID (64 hex digits). Where the
  ledger knows an ID, a unique prefix of at least 8 hex digits is enough.
  Commands that add someone to the ledger (`enroll`, `admins --add`,
  `rule --owner`, `rule --signer`, `trust --signer`, `--peer`) need the
  full key ID.
- For host destinations and residency allowlists, a full lowercase key ID
  takes precedence over display names, and an unknown full ID never falls
  back to a name. Names and permitted prefixes must identify a single
  enrolled host across both forms. Ambiguous shorthand is refused; use
  the full key ID. Compute-job host lists accept names or full IDs only.
- `MODULE` is a `.wasm` file or a module hash (64 hex digits).
- Passphrases of admin, owner and signer keys are read from the terminal,
  or from the first line of `--passphrase-file FILE`. That one passphrase
  is used for every key of the command.

> [!NOTE]
> `serve` names its state directory with `--state`. `run`, `list` and
> `call` name theirs with `--state-dir`. The layouts differ: see
> [state directories](configuration.md#state-directories).

## paglets-host

```text
paglets-host --version | --info
paglets-host run | list | call ...
paglets-host keys | mesh | module | ledger ...
paglets-host serve ...
paglets-host remote ...
```

| Command | Prints |
|---|---|
| `--version` | `paglets-host <version>` |
| `--info` (also with no arguments) | version, platform, compiler, whether reflection is on, the WAMR release and mode, the paglet ABI and the mesh protocol |

Example output of `--info`:

```text
paglets-host 0.2.0
  platform:   macos arm64
  compiler:   gcc 16.1.0
  reflection: yes
  runtime:    WAMR-2.4.5 (fast-interp, hardware bound checks)
  paglet ABI: v1
  protocol:   mesh v2
```

### run, list and call

These commands run paglets on this machine alone, without a mesh.

```text
paglets-host run <module.wasm> [--args JSON] [--call NAME [JSON]]...
                 [--state-dir DIR] [--threads N] [--keep] [--in-process] [--no-sandbox]
                 [--root NAME=DIR]...
paglets-host list --state-dir DIR
paglets-host call --state-dir DIR <paglet-id|all> NAME [JSON] [--expect TEXT]
```

`run` creates one paglet from the module and sends it the calls in order.
It prints `paglet <id> (module <hash prefix>)` and one line
`NAME -> <reply>` per call. Then it waits until the paglet is idle and
disposes of it, unless `--keep` is given. It exits with status 1 if any
call failed.

`list` resumes the paglets of a state directory and prints one line per
paglet: ID, trust class, state, module hash prefix and owner.

`call` resumes the paglets of a state directory and sends one message to
one paglet, or to every paglet with `all`. It prints
`<id> NAME -> <reply>` per paglet.

| Option | Commands | Meaning |
|---|---|---|
| `--args JSON` | `run` | arguments of the new paglet (its `on_created`) |
| `--call NAME [JSON]` | `run` | a request to send after creation, with an optional JSON body; repeatable. A following argument that starts with `--` is not taken as the body. |
| `--state-dir DIR` | all | durable state: paglets outlive the process and resume from their last memory image. Without it, everything stays in memory. |
| `--keep` | `run` | keep the paglet instead of disposing of it at the end |
| `--expect TEXT` | `call` | fail (status 1) unless every printed reply contains `TEXT` |
| `--threads N` | all | scheduler lanes, and worker processes (default 2; 0 uses one lane) |
| `--in-process` | all | run paglets inside the host process instead of in `paglets-worker` |
| `--no-sandbox` | all | run worker processes without the operating-system sandbox (for debugging) |
| `--root NAME=DIR` | all | a named root of the `files` service; repeatable |

Worker execution is the default. `paglets-worker` must be installed beside
`paglets-host`; startup checks that it is executable and completes a worker
protocol exchange within five seconds. A missing or failing worker stops
startup. Use `--in-process` explicitly to run guests in the host process.
If a requested OS sandbox cannot be applied, the worker also refuses to
start; `--no-sandbox` is the explicit opt-out. The host logs its actual
execution mode.

Paglets run in `paglets-worker` processes when that program is found next
to `paglets-host`, and inside the host process otherwise.

### keys

```text
paglets-host keys init --role admin|owner|host|signer --name NAME --out FILE
                       [--passphrase-file FILE] [--kdf moderate|interactive]
paglets-host keys show FILE
```

`keys init` makes an Ed25519 key, writes it to a new file (it refuses to
overwrite one) and prints the key ID.

- Admin, owner and signer keys need a passphrase. They are encrypted with
  XChaCha20-Poly1305 under a key derived by Argon2id.
- `--kdf` chooses libsodium's Argon2id limits: `moderate` (the default) or
  `interactive` (faster, weaker; the tests use it).
- Host keys are written unencrypted with owner-only permissions. With
  `--passphrase-file` they are encrypted too, but `serve` cannot open an
  encrypted host key.

`keys show` prints the key ID, role, name and whether the file is
encrypted. It needs no passphrase.

### mesh create

```text
paglets-host mesh create --name NAME --ledger DIR --admin KEY... [--admin-quorum N] [--quorum N]
```

Creates a new mesh: its genesis record, in a ledger directory that must be
empty or not exist yet. `--admin` names the first admin keys (repeatable).
`--admin-quorum` is the number of admin signatures that changes of the
admin set need, and `--quorum` the number for every other admin record.
The admin-set quorum defaults to 2 with three or more initial admins,
otherwise 1. The other quorum defaults to 1. Explicit options override
these defaults. A mesh with several admins and an admin-set quorum of 1
prints a note to stderr: any one admin can change the admin set. Stdout
contains only the mesh ID. Existing ledgers keep their recorded quorums;
change them with `ledger admins`, signed under the current admin-set quorum.

### module inspect

```text
paglets-host module inspect MODULE.wasm [--json]
```

Prints the module's hash and size, its initial and largest memory, whether
it is a paglet and for which trust classes, its imports and exports, and
its [interface description](interface-descriptions.md) (`--json`: the
description as JSON).

The import/trust report checks function kinds as well as permitted names.
A table, memory, global or tag import is refused even if its name matches
an allowed function.

### ledger

All ledger commands work on a local ledger directory (`--ledger DIR`): an
admin's copy or a host's. Commands that sign as an admin take
`--admin KEY`. Give `--admin` several times when a record needs more than
one admin's signature. Each command that adds a record prints its ID. If
the record has no effect yet (for example, it still lacks signatures), a
note says why.

```text
paglets-host ledger show --ledger DIR [--ignored]
paglets-host ledger request --ledger DIR --key KEY [--label L]...
paglets-host ledger approve --ledger DIR --admin KEY... REQUEST [--label L | --group G]... [--host-only]
paglets-host ledger deny --ledger DIR --admin KEY... REQUEST [--reason TEXT]
paglets-host ledger enroll --ledger DIR --admin KEY... host|owner KEY-ID NAME [--label L | --group G]...
paglets-host ledger remove --ledger DIR --admin KEY... host|owner KEY-ID
paglets-host ledger revoke --ledger DIR --admin KEY... KEY-ID|RECORD-ID [--reason TEXT]
paglets-host ledger revoke --ledger DIR --admin KEY... --module MODULE [--reason TEXT]
paglets-host ledger admins --ledger DIR --admin KEY... [--add KEY-ID]... [--remove KEY-ID]...
                           [--admin-quorum N] [--quorum N]
paglets-host ledger sign --ledger DIR --admin KEY... RECORD-ID
paglets-host ledger rule --ledger DIR --admin KEY... --name NAME --decision allow|ask|deny
                         --service SERVICE --op OP... [match and scope options]
paglets-host ledger audit --ledger DIR
paglets-host ledger sign-module --ledger DIR --key SIGNER-KEY --name NAME [--version V] MODULE
paglets-host ledger trust --ledger DIR --admin KEY... --name NAME --class C... [--signer KEY-ID]... [--module MODULE]...
paglets-host ledger module-policy --ledger DIR --admin KEY... any|trusted
paglets-host ledger residency --ledger DIR --admin KEY... ROOT host-only|none
paglets-host ledger residency --ledger DIR --admin KEY... ROOT hosts [HOST]... [--host-label L]...
```

| Command | Does |
|---|---|
| `show` | prints the mesh, quorum, admins, hosts, owners, pending requests, rules, grant requests, grants, module trust and signatures, residency rules and revocations. `--ignored` lists the records without effect, with the reason. |
| `request` | adds an enrollment request signed by a host or owner key (`--label`: the labels a host asks for). Admin and signer keys are not enrolled by request. Prints the request ID. |
| `approve` | decides an enrollment request or a grant request. For a host, `--label` replaces the requested labels; for an owner, `--group` sets the groups. For a grant request, each item becomes a grant for the requested duration, on every host, or only on the requesting host with `--host-only`. |
| `deny` | rejects an enrollment or grant request |
| `enroll` | enrolls a host (with `--label`) or an owner (with `--group`) directly, by key ID |
| `remove` | removes an enrolled host or owner |
| `revoke` | revokes a key or a record. Admins are removed with `admins --remove` instead. With `--module`, revokes a module: hosts end the paglets that run it. |
| `admins` | adds or removes admins, and changes the quorums |
| `sign` | adds the given admins' signatures to an existing record, for records that need a quorum of several admins |
| `rule` | adds a policy rule (see below) |
| `audit` | prints audit records held in the local ledger copy (after `remote pull`, `remote approve` or `remote audit`); `remote audit` pulls first |
| `sign-module` | signs a module as `NAME` (and `--version`) with a signer key. Host keys cannot sign modules. |
| `trust` | lets modules run in the trust classes `--class` (`roaming`, `resident`, `system`; repeatable): the modules signed by `--signer`, or the modules named by `--module`. At least one of the two is needed. |
| `module-policy` | `trusted`: roaming paglets need module trust as well; `any`: they do not |
| `residency` | a data residency rule for the named root `ROOT`: `host-only`, `hosts` (named by host name or key ID, or `--host-label`), or `none` to lift the rule |

Options of `ledger rule`:

| Option | Meaning |
|---|---|
| `--name NAME` | the rule's name (required) |
| `--decision D` | what the rule decides: `allow`, `ask` or `deny` (required) |
| `--service SERVICE` | the service it covers, for example `files`, `web`, `locator` (required) |
| `--op OP` | an operation, or for `files` a right (`read`, `write`, `create`, `delete`); repeatable, at least one |
| `--owner KEY-ID` | match: paglets of this owner; repeatable |
| `--group G` | match: paglets of owners in this group; repeatable |
| `--module HASH` | match: paglets of this module; repeatable |
| `--signer KEY-ID` | match: paglets whose module this signer signed; repeatable |
| `--trust T` | match: paglets of this trust class; repeatable |
| `--host-label L` | match: only on hosts with this label; repeatable |
| `--root R` | scope: a named root; repeatable |
| `--path PATTERN` | scope: a path pattern below the root (`*`, `?`, `**`); repeatable |
| `--max-duration MS` | grants derived from an `allow` rule expire at most this many milliseconds after they are issued; an upper bound beyond the timestamp range is treated as its last representable value |
| `--priority N` | the highest priority wins when several rules match (default 0) |

Options without a value match everything. The
[policy design](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-policy.md)
explains how the most specific rule is chosen.

### serve

```text
paglets-host serve --key HOST-KEY --ledger DIR --state DIR [options]
```

Runs a host of the mesh whose ledger copy is in `--ledger`. It prints its
key ID, the mesh ID and its URL, then runs until SIGINT or SIGTERM, or
until the stop file appears. `--key`, `--ledger` and `--state` are
required. The host key must be unencrypted.

| Option | Default | Meaning |
|---|---|---|
| `--key FILE` | | the host key |
| `--ledger DIR` | | the host's copy of the ledger |
| `--state DIR` | | the host's state: `runtime/`, `services/` and `node/` below it |
| `--listen HOST:PORT` | `127.0.0.1:0` | the address of the HTTPS server; port 0 picks a free port |
| `--advertise URL` | `https://<listen host>:<port>` | the URL announced to other hosts |
| `--no-listen` | | no inbound port: other hosts reach this one through relays |
| `--join URL` | | a bootstrap contact: any enrolled host; tried until it answers; repeatable |
| `--peer KEY-ID=URL` | | a seed host with its address; repeatable |
| `--no-beacon` | beacons on | no multicast beacons on the local network |
| `--beacon-port N` | 47471 | the UDP port of the beacons |
| `--tls-cert FILE`, `--tls-key FILE` | a new self-signed certificate at each start | PEM certificate (optionally followed by its intermediate CA certificates) and unencrypted key of the HTTPS server; give both |
| `--tls-ca FILE` | not checked | check the certificates of the hosts this one connects to against only these CA certificates (PEM), not the system's |
| `--root NAME=DIR` | | a named root of the `files` service; repeatable |
| `--module-source DIR` | | a directory whose `.wasm` files the host may load when it lacks a module; repeatable |
| `--aot-compiler PATH` | beside the host | compiler for job modules |
| `--no-aot` | | run compute jobs in the interpreter |
| `--slots N` | | compute slots this host offers (`compute-slots`); a positive number |
| `--threads N` | 2 | scheduler lanes and worker processes; 0 uses one lane |
| `--memory-mb N` | 0, none | opt into a per-paglet memory ceiling, 1 to 4096 MB; 0 leaves requirements unknown, within Wasm32's 4 GB address space |
| `--handler-ms N` | -1, off | optional watchdog per handler invocation; positive elapsed milliseconds |
| `--job-max-ms N` | -1, none | optional elapsed-time ceiling for compute jobs; positive milliseconds |
| `--job-max-memory-mb N` | 0, none | optional job memory ceiling; explicit declarations that exceed it are rejected before execution |
| `--require-job-resources` | off | require finite positive execution-time and memory declarations for jobs |
| `--require-paglet-memory` | off | require a Wasm memory maximum declaration; set it with `paglets_add_module(... MEMORY_MB N)` |
| `--in-process` | | run paglets inside the host process |
| `--no-sandbox` | | worker processes without the operating-system sandbox |
| `--stop-file FILE` | | stop when this file appears |

Gateway options (see [System paglets](system-paglets.md#gateways-to-the-web-and-to-ai)):

| Option | Default | Meaning |
|---|---|---|
| `--web` | off | offer the `web` system paglet |
| `--web-internal URL-PREFIX` | none | an internal destination that `web` may reach; repeatable |
| `--web-no-internet` | internet allowed | only the `--web-internal` destinations |
| `--web-proxy URL` | `HTTPS_PROXY`, `https_proxy`, `HTTP_PROXY` or `http_proxy` | the proxy for `web` requests |
| `--web-search URL` | none | a SearXNG-style JSON search endpoint for `search` (`{query}` is replaced) |
| `--web-ca FILE` | the system's CA certificates | more CA certificates (PEM), trusted besides the system's, for the HTTPS sites that `web` fetches, for example of an internal CA |
| `--ai BACKEND` | off | offer the `ai` system paglet through `ollama`, or through the `test` backend with fixed answers |
| `--ai-url URL` | `http://127.0.0.1:11434` | where Ollama listens |
| `--ai-model NAME` | every model Ollama has | a model to offer; repeatable |

A host whose key is not enrolled in its ledger copy still starts. It
treats its `--join` and `--peer` hosts as seeds and waits for its
enrollment (see [Configuration and troubleshooting](configuration.md#a-host-is-not-enrolled-yet)).

### remote

```text
paglets-host remote COMMAND --connect URL --key KEY --ledger DIR [--host-key KEY-ID] [arguments]
```

`remote` opens an end-to-end Noise channel to the host at `--connect`, as
the admin or owner whose key is in `--key`. The role comes from the key
file. It accepts the host only if it is enrolled in the ledger copy
`--ledger`, or if its key is `--host-key`. An explicit pin supports
bootstrapping an unenrolled host, but never overrides a key revocation
already recorded in that ledger copy.

TLS carries the channel, but `remote` does not check the server certificate
and has no CA option. Host authentication uses the key in the Noise
handshake, checked against the ledger copy or the explicit `--host-key` pin.

Known session replies are checked before the command prints results. A
missing required field, a wrong field type, or a malformed list entry
returns `malformed answer` and exit status 1. Invalid entries are not
silently skipped and malformed replies do not produce partial result
output. Optional metadata from newer hosts may be absent on older ones;
unknown extension fields are accepted.

| Command | Arguments | Does |
|---|---|---|
| `status` | | the host's name, key, mesh, ledger size, version and uptime, system (OS, CPUs, memory), its native system paglets, and its paglets with state, module, owner and trust class |
| `moves` | `[clear-cache]` | JSON counters for moves, pages, compressed bytes and phase timings in nanoseconds; `clear-cache` clears cached image pages (admin only, refused while a transfer is pending), preserving counters and modules |
| `push` | | sends every record of the ledger copy to the host |
| `pull` | | adds the host's ledger records to the ledger copy |
| `launch` | `MODULE [--args JSON] [--nonce NONCE] [--hours N]` | starts a paglet of the owner on the host, from a `.wasm` file (sent along) or the hash of a module the host has. The passport is signed locally and is valid for positive `--hours` (default 24); an expiration outside the signed Unix-millisecond range is refused before sending a launch request. The nonce is random unless given (1–128 bytes); the printed global ID derives from mesh, owner and nonce. Use a fresh nonce for each new paglet. `--id` is refused; see [identity upgrades](hosts-and-meshes.md#paglet-identities-and-upgrades). |
| `call` | `PAGLET NAME [JSON] [--expect TEXT] [--timeout SECONDS]` | sends a request to a paglet on the connected host and prints the reply; a paglet that moved away cannot be called there (`locate` finds its current host); status 1 for an error reply, or when the reply lacks `TEXT`. The host waits up to 30 s by default (a paglet that is moving replies after the move), then prints `error: timeout`. `--timeout SECONDS` accepts decimal seconds from 0.1 to 120, with at most three decimal places (for example `--timeout 37.5`). Invalid values are rejected before connecting |
| `send` | `PAGLET NAME [JSON] [--priority N]` | sends a one-way message to a paglet on the connected host (priority 0 to 7, default 3); prints `sent` once the host queued it |
| `describe` | `PAGLET` or `--module HASH`, `[--json]` | the [interface description](interface-descriptions.md) of the paglet's module (of a native system paglet: of its service), or of a module on the host |
| `dispatch` | `PAGLET DESTINATION` | moves a paglet to a host name/key ID, `label:L`, `offer:SERVICE[.OP]` or `any`; optional query parameters are `retries=N` and `arrival=active` or `arrival=inactive` (see [Hosts and meshes](hosts-and-meshes.md#working-with-live-hosts)). There is no retract operation: the owner or an admin uses this command to dispatch it back. It waits until the move ended, at most 120 s: prints `moved to NAME KEY`, or exits with status 1 and `the move failed, the paglet stays: REASON` |
| `dispose` | `PAGLET` | ends a paglet (its owner or an admin); `bad_state` while a dispatch, arrival or end hook is pending; retry after the transition |
| `terminate` | `PAGLET [--reason TEXT]` | ends a paglet as failed at once, also while a handler runs (admins) |
| `hosts` | | the hosts as this host knows them: name, key, state (`this host`, `online`, `one-way` or `offline`), address, protocol and ABI, relays, `INCOMPATIBLE` for hosts of another protocol or ABI, and `send failed: REASON` when this host's frames to it fail |
| `landscape` | | `mesh-info`'s view of every host: CPUs, load, free memory, compute slots, queue, paglets, and its offers |
| `jobs [cancel JOB]` | | list local queued/running jobs and progress, or cancel a job; owners see and cancel their own jobs, admins all |
| `slots` | | the host's compute slots: leases and queue; each lease includes `granted_ms` and `expires_ms` as Unix milliseconds |
| `zombies` | `list`, `cancel GROUP`, `redirect GROUP SINK`, `traces GROUP`, `check GROUP`, `result GROUP` | the zombie groups the host knows ([The zombie protocol](zombie-protocol.md)): `list` shows each group's state (`running`, `delivered`, `cancelled`), sink and replica here; `cancel` ends a group on every host, also on hosts that are offline now, once they return; `redirect` sends its result to another sink (`host:NAME`, where NAME is a ledger name, full host key ID or at least 8 hex digits of it); `traces` prints the protocol steps that every live host recorded, in time order; `check` checks them against the specification and exits with status 1 if a step violates it; `result` prints the result this host accepted as the group's sink (to standard output; what it is to standard error). Owners see their own groups, admins all. |
| `locate` | `PAGLET` | where a paglet is, from any host |
| `pin` | `PAGLET [--minutes N] [--reason TEXT]` | keeps a paglet where it is (default 10 minutes); positive minutes are capped at 1440 (24 h) before conversion |
| `pins` | | the pins on the host |
| `unpin` | `PAGLET [--pin ID]` | ends the paglet's pins, or one pin, wherever it is (admins) |
| `requests` | | pulls, then lists pending enrollment and grant requests |
| `approve` | `REQUEST [--label L]... [--group G]... [--host-only] [--admin KEY]...` | pulls, signs the decision with the admin key (and further `--admin` keys) and pushes it |
| `deny` | `REQUEST [--reason TEXT]` | the same for a rejection |
| `audit` | | pulls, then prints the audit log; see `ledger audit` for the local-copy form |
| `modules` | | the host's modules: hash, size, paglets using it, pinned, signed names |
| `push-module` | `MODULE.wasm` | stores a module on the host; prints its hash |

`--passphrase-file` works here as for the ledger commands. The CLI waits
up to 150 s for each answer of the host.

## paglets-web

```text
paglets-web --connect URL --key KEY --ledger DIR [--host-key KEY-ID] [--passphrase-file FILE]
            [--port N] [--port-range A-B] [--no-port-search] [--listen ADDR] [--allow-remote]
            [--no-browser] [--module-dir DIR] [--refresh-ms N] [--program NAME]
```

Serves a web UI for the mesh on this computer and opens it in the
browser. The session options are those of `remote`. [The web UI](web-ui.md)
describes the pages, the options, the port search, the security model and
the JSON API. `--help` prints the usage, `--version` the version.
The UI's [Pages section](web-ui.md#pages) lists the operations that require
the CLI, including ledger changes, job management and detailed zombie traces.

## paglets-worker

`paglets-host` starts one `paglets-worker` per scheduler lane and passes
it the ends of its IPC channels on the command line. It is not meant to be
started by hand: without those arguments it prints
`paglets-worker is started by paglets-host` and exits with status 2.

```text
paglets-worker --check-sandbox
```

`--check-sandbox` enters the sandbox and checks that it refuses what it
must (opening files, creating sockets, starting programs). It prints
`sandbox ok` and exits with status 0, or prints the reason and exits with
status 1. The CTest test `worker_sandbox` runs it.

## paglets-spike

`paglets-spike` is the feasibility tool of milestone M0. It drives
modules directly, without the paglets runtime around them, and holds the
measurements of M0 and M1:

```text
paglets-spike info <module.wasm>
paglets-spike image-save <counter.wasm> <image> [--increments N] [--bloat KB]
paglets-spike image-resume <counter.wasm> <image> [--increments N] [--expect VALUE]
paglets-spike bench <testbed.wasm> <counter.wasm> [--instances N]
paglets-spike runtime <counter.wasm> <ping_pong.wasm> [--threads N] [--worker PATH]
paglets-spike call-bench <module.wasm> <message> [--count N]
```

`image-save` and `image-resume` move a counter paglet as a memory image
between processes, or between machines of different architectures: the
module must be the same file on both sides.

```bash
S=build/linux-gcc16/host/paglets-spike
$S image-save build/linux-gcc16/guests/counter.wasm counter.pgimg --increments 5
$S image-resume build/linux-gcc16/guests/counter.wasm counter.pgimg --increments 2 --expect 18
```

The results are in the
[M0](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-m0-results.md) and
[M1](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-m1-results.md)
result documents.

A committed arrival whose required state image cannot be written ends as
failed and reports a failed arrival receipt. It cannot resume as a
successful arrival after restart. Move statistics distinguish destination
host-record preparation from instance activation and image restoration.
