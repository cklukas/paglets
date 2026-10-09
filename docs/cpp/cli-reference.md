# Command-line reference

paglets/cpp builds three programs into `build/<preset>/host/`:

| Program | Purpose |
|---|---|
| `paglets-host` | runs paglets on one host, manages keys and the mesh ledger, runs a host of a mesh, and talks to live hosts |
| `paglets-worker` | runs paglet instances for `paglets-host`, in an operating-system sandbox |
| `paglets-spike` | the measurement and memory-image tool of milestones M0 and M1 |

The examples use `H=build/linux-gcc16/host/paglets-host`.

## Conventions

- Options take their value as the next argument (`--state-dir DIR`, not
  `--state-dir=DIR`). Options marked "repeatable" may be given several times.
- An unknown option, a missing value or a missing argument prints the usage
  of the command group to stderr and exits with status 2.
- Other failures print `paglets-host: <reason>` and exit with status 1.
- Message bodies and arguments are JSON. The host converts them to
  MessagePack for the paglet and prints replies as JSON (`ok` for an empty
  reply, `error: <code>` for an [error code](configuration.md#error-codes)).
- `KEY` is a key file, `KEY-ID` a key's ID (64 hex digits). Where the
  ledger knows an ID, a unique prefix of at least 8 hex digits is enough.
  Commands that add someone to the ledger (`enroll`, `admins --add`,
  `rule --owner`, `rule --signer`, `trust --signer`, `--peer`) need the
  full key ID.
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
paglets-host 0.1.0
  platform:   macos arm64
  compiler:   gcc 16.1.0
  reflection: yes
  runtime:    WAMR-2.4.5 (fast-interp, hardware bound checks)
  paglet ABI: v1
  protocol:   mesh v1
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
| `--threads N` | all | scheduler lanes, and worker processes (default 2) |
| `--in-process` | all | run paglets inside the host process instead of in `paglets-worker` |
| `--no-sandbox` | all | run worker processes without the operating-system sandbox (for debugging) |
| `--root NAME=DIR` | all | a named root of the `files` service; repeatable |

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
Both default to 1. Prints the mesh ID.

### module inspect

```text
paglets-host module inspect MODULE.wasm
```

Prints the module's hash and size, its initial and largest memory, whether
it is a paglet and for which trust classes, and its imports and exports.

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
| `audit` | prints the audit log: every policy decision the hosts recorded |
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
| `--max-duration MS` | grants derived from an `allow` rule expire at most this many milliseconds after they are issued |
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
| `--tls-cert FILE`, `--tls-key FILE` | a self-signed certificate | PEM certificate and key of the HTTPS server; give both |
| `--tls-ca FILE` | not checked | CA certificates (PEM) that other hosts' certificates must chain to |
| `--root NAME=DIR` | | a named root of the `files` service; repeatable |
| `--module-source DIR` | | a directory whose `.wasm` files the host may load when it lacks a module; repeatable |
| `--slots N` | | compute slots this host offers (`compute-slots`); a positive number |
| `--threads N` | 2 | scheduler lanes and worker processes |
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
| `--web-ca FILE` | the system's CA certificates | CA certificates (PEM) for checking the HTTPS sites that `web` fetches, for example of an internal CA |
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
`--ledger`, or if its key is `--host-key`.

| Command | Arguments | Does |
|---|---|---|
| `status` | | the host's name, key, mesh, ledger size, and its paglets |
| `push` | | sends every record of the ledger copy to the host |
| `pull` | | adds the host's ledger records to the ledger copy |
| `launch` | `MODULE.wasm [--args JSON] [--id PAGLET-ID] [--hours N]` | starts a paglet of the owner on the host. The passport is signed locally and is valid for `--hours` (default 24). The ID is random unless given. Prints the paglet ID. |
| `call` | `PAGLET NAME [JSON] [--expect TEXT]` | sends a request to a paglet and prints the reply; status 1 for an error reply, or when the reply lacks `TEXT` |
| `dispatch` | `PAGLET DESTINATION` | moves a paglet with a transfer ticket (see [Hosts and meshes](hosts-and-meshes.md#working-with-live-hosts)) |
| `dispose` | `PAGLET` | ends a paglet (its owner or an admin) |
| `hosts` | | the hosts as this host knows them: name, key, online state, address, protocol and ABI, relays, and `INCOMPATIBLE` for hosts of another protocol or ABI |
| `landscape` | | `mesh-info`'s view of every host: CPUs, load, free memory, compute slots, queue, paglets, and its offers |
| `slots` | | the host's compute slots: leases and queue |
| `locate` | `PAGLET` | where a paglet is, from any host |
| `pin` | `PAGLET [--minutes N] [--reason TEXT]` | keeps a paglet where it is (default 10 minutes) |
| `pins` | | the pins on the host |
| `unpin` | `PAGLET [--pin ID]` | ends the paglet's pins, or one pin, wherever it is (admins) |
| `requests` | | pulls, then lists pending enrollment and grant requests |
| `approve` | `REQUEST [--label L]... [--group G]... [--host-only] [--admin KEY]...` | pulls, signs the decision with the admin key (and further `--admin` keys) and pushes it |
| `deny` | `REQUEST [--reason TEXT]` | the same for a rejection |
| `audit` | | pulls, then prints the audit log |
| `modules` | | the host's modules: hash, size, paglets using it, pinned, signed names |
| `push-module` | `MODULE.wasm` | stores a module on the host; prints its hash |

`--passphrase-file` works here as for the ledger commands.

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
