# The web UI: paglets-web

`paglets-web` shows a mesh in the browser and lets you work with it:

- the hosts, with their properties and their counts of system and user paglets;
- each host's details and running paglets;
- forms for sending commands to paglets that describe their messages;
- launching modules with start arguments;
- moving, pinning and ending paglets.

Next to every view and action, it shows the `paglets-host remote` command
that does the same in a shell.

It is a mesh client, like `paglets-host remote`: it opens end-to-end
sessions with the hosts as an admin or owner. It serves its pages itself
on this computer, without a web server or library. Nothing is installed on
the hosts; they only need to run `paglets-host serve`.

## Starting it

```bash
build/macos-arm64/web/paglets-web --connect https://mac.local:7443 --key olga.key --ledger ledger \
    --module-dir build/macos-arm64/guests
```

It asks for the key's passphrase (or reads `--passphrase-file`), binds a
port on `127.0.0.1`, prints the page's address and opens it in the default
browser:

```text
paglets-web: http://127.0.0.1:8470/?t=5421d9822f458c0804290ff9adc792b553f93c84fe4b59fe
```

The link carries a token for this start of `paglets-web`. The browser
exchanges it for a cookie on its first visit. Other browsers and programs
on the computer cannot use the API without the token. Stop `paglets-web`
with Ctrl-C.

| Option | Default | Meaning |
|---|---|---|
| `--connect URL` | required | the entry host: its registry names the other hosts, and its `mesh-info` describes them all |
| `--key KEY` | required | an admin or owner key; launching paglets needs an owner key |
| `--ledger DIR` | required | the ledger copy that names the mesh's hosts and owners |
| `--host-key KEY-ID` | from the ledger | accept the entry host by its key instead of by the ledger copy |
| `--passphrase-file FILE` | the terminal | the key's passphrase, from the file's first line |
| `--port N` | 8470, see below | listen on exactly this port (`0`: any free port) |
| `--port-range A-B` | `8470-8499` | where to look for a free port |
| `--no-port-search` | search | only the first port of the range |
| `--listen ADDR` | `127.0.0.1` | the address to listen on; any other than `127.0.0.1`, `::1` or `localhost` needs `--allow-remote` |
| `--allow-remote` | off | allow other addresses, and any `Host` header |
| `--no-browser` | open it | only print the address |
| `--module-dir DIR` | none | `.wasm` files offered for launching and pushing (files can also be uploaded from the browser) |
| `--refresh-ms N` | 2000 | how often the hosts are asked (at least 500) |
| `--program NAME` | `paglets-host` | the program name in the commands shown |

### Ports

Without `--port`, `paglets-web` tries port 8470, then the rest of
`--port-range`, and finally any free port that the system assigns. It
finds a free port by binding it, so two instances started at the same time
get different ports. With `--port N`, or with `--no-port-search`, it
listens on exactly that port, or exits with status 1 if the port is in use.

## Pages

**Mesh.** The welcome page shows the mesh's name and entry host, and
totals: hosts online and enrolled, system paglets, user paglets. A table
lists every host:

- its state: online, one-way or offline;
- labels, OS and architecture, CPUs;
- load per CPU, free memory and free compute slots;
- its system and user paglets;
- its version and uptime.

Click a host for its page.

**Host.** The host's properties: key, address, versions, uptime, system,
CPU model, memory, paglet storage and compute slots. Further sections:

- its links to the other hosts, and its offers;
- its user paglets, with their module, owner, state and trust class;
- its system paglets, where expanding one lists the operations it offers;
- the modules it stores, and its pins.

**Paglet.** A form for each message the paglet's module describes:

- fields come from the message's request type;
- a JSON tab edits the same value as text;
- **Call** sends a request and shows the reply, **Send** a one-way message
  with a priority; sending a `both` message runs its handler without a reply
  capability, so the caller receives no reply;
- messages marked as dangerous ask before they are sent;
- *Other message* sends any message by name with a JSON payload.

Actions:

- **Dispatch** moves the paglet with a transfer ticket (`b`, `label:L`,
  `any`) and follows it to its new host;
- **Pin** keeps it in place for some minutes;
- **Locate** finds it anywhere in the mesh;
- **Dispose** ends it;
- **Terminate** ends it as failed (admins).

A log lists what this page sent.

**Modules.** The modules that the hosts store and that `--module-dir`
holds: name (from module signatures or the interface description), hash,
size, which hosts have it, and whether it is described. **Launch** opens a
form with the target host, the start arguments (a form made from the
module's `launch` type), an optional owner-local nonce and how long the
passport is valid. The mesh, owner and nonce determine the global paglet
ID; leaving the nonce blank generates a fresh identity. Launch needs an
owner key; with an admin key the form explains the requirement and the
API refuses the request. **Push only** stores a module file on a host without starting
it. **Upload a module** takes a `.wasm` file from this computer: the browser
reads its interface description, and the module travels with the launch
or push.

**Zombies.** The [zombie groups](zombie-paglets.md) that the hosts know,
each with:

- its state (running, delivered, cancelled), sink and replicas;
- **Check**, which compares the protocol steps every host recorded with
  the specification;
- **Cancel**, which ends the group on every host;
- **Redirect**, which sends its result to another sink.

Owners see their own groups, admins all.

Every page refreshes itself without losing what you typed or which
sections you opened.

The UI covers part of `paglets-host remote`. Use the CLI for ledger
operations (`push`, `pull`, `requests`, `approve`, `deny`, `audit`),
`unpin`, compute lease/queue details (`slots`), compute job listing and
cancellation (`jobs`), and zombie `traces` and `result`. Slot counts are
visible on host pages, but those commands have no form or result page.
Some have copyable commands in **Shell** boxes; run them in a terminal
(see the [remote command reference](cli-reference.md#remote)).

> [!NOTE]
> Forms need an [interface description](interface-descriptions.md) in the
> module. Paglets without one still get *Other message* (name and JSON),
> and modules without one are launched with JSON arguments.

### Trying it on real machines

`cpp/tools/web_live.sh` builds a fresh mesh of this machine and remote
Linux hosts (over SSH, as `mesh_bench.sh` does). It starts a counter with a
start value on every host, hello, and a Mesh Chronicle, then runs
`paglets-web` against the mesh. It checks the API (every host answers with
its status, and a call through the UI reaches a paglet on a remote host)
and prints the page's address:

```bash
cpp/tools/web_live.sh --remote pi=klukas@rpi4:/home/klukas/paglets-live/cpp/build/linux-gcc16/host \
    --remote uconsole=uconsole:/home/klukas/paglets-live/cpp/build/linux-gcc16/host
```

`--check-only` stops after the checks; otherwise Ctrl-C ends `paglets-web`
and the mesh. The driver needs Bash 4+, jq, curl and SSH, with Linux remote
hosts reachable directly over IPv4. Use names and absolute build paths without
spaces or shell metacharacters. On macOS, set `TMPDIR` to a unique directory
below the mounted `/Volumes/S8T/tmp` or `/Volumes/PRO-BLADE/tmp` first.

Each run creates its own local workspace and a matching directory below each
remote user's `tmp/`. It refuses to start while a `paglets-host serve` process
already runs on any selected host. Cleanup signals only its recorded web
process after checking its executable and workspace, and stops its hosts via
their own stop files. It verifies that the recorded host processes have ended
before removing their directories. If SSH fails or process identity cannot be
confirmed, it reports the retained workspace for manual cleanup and exits with
an error. Host and web logs and process records remain in the printed local
`-logs` directory.

## The commands next to the views

Every panel has a **Shell** box with the matching command, for example
`paglets-host remote status` on a host page. Every form keeps its command
up to date while you type, and every result shows the command that does
exactly what was just done:

```text
paglets-host remote call --connect https://127.0.0.1:17443 --key olga.key --ledger ledger 9126f94a165ae1d339eaf101e3f7f912 increment '{"by":1,"note":"from the web","mode":"add"}'
```

The commands use the key file, ledger directory and passphrase file that
`paglets-web` was started with. A host other than the entry host is named
by its address. **Copy** puts a command on the clipboard.

## Security

Connections from `paglets-web` to mesh hosts use the shared session client.
It does not check their TLS certificates and has no CA option. The Noise
handshake authenticates the host key against the ledger copy or an explicit
host-key pin. This is separate from the local browser-to-UI connection.

`paglets-web` acts with your key, so it protects its API from other
programs and websites:

- it listens on `127.0.0.1` by default; `--listen ::1` and
  `--listen localhost` are also allowed. Other addresses require
  `--allow-remote`;
- the API needs the token: the HttpOnly, `SameSite=Strict` cookie that the
  printed link sets, or an `Authorization: Bearer TOKEN` header;
- the `Host` header must name `127.0.0.1`, `localhost` or `[::1]` with
  the port, which stops DNS rebinding (except with `--allow-remote`);
- requests that change something are POSTs with a JSON body. A request
  whose `Origin` is not the page's own, or whose `Sec-Fetch-Site` says it
  is cross-site, is refused;
- every response forbids framing and inline scripts (Content Security
  Policy) and is not cached.

The hosts check every request as they check `paglets-host remote`. An
owner sees and acts on their own paglets, an admin on all of them.

The integration test `web: the UI shows the mesh, launches, calls and ends
paglets` (`cpp/tests/test_web.cpp`) starts two mesh hosts and the real HTTP
API server. It checks launch nonces and copied commands, describe, send,
call, bytes payloads, module upload, zombies and disposal, plus refusals
without the token, with another Host header and from another origin.

## JSON API

The pages use a JSON API, which also works with `curl` and the token:

```bash
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8470/api/mesh
```

| Request | Answer |
|---|---|
| `GET /api/mesh` | the model of the welcome page: `mesh`, `me`, `totals`, and `hosts` with each host's registry entry, `snapshot` (mesh-info), `status`, `counts` and `shell` commands; `version` changes when the content changes |
| `GET /api/host?key=KEY-ID` | a host of the model with its `modules` and `pins` |
| `GET /api/paglet?host=KEY-ID&id=PAGLET` | the paglet's `interface` description and `shell` commands |
| `GET /api/modules` | the modules of the hosts and of `--module-dir` with their interfaces |
| `GET /api/zombies` | the zombie groups of every host, with state and replicas |
| `POST /api/call` | `{host, paglet, name, payload?, timeout_ms?}` → `{status, status_name, reply, shell}` |
| `POST /api/send` | `{host, paglet, name, payload?, priority?}` → `{sent, shell}` |
| `POST /api/launch` | `{host, module (hash), file (in --module-dir) or name and data (an upload, base64), args?, nonce?, hours?}` → `{paglet, host, shell}` |
| `POST /api/push-module` | `{host, file}` or `{host, name, data}` → `{hash, shell}` |
| `POST /api/dispatch` | `{host, paglet, destination}` → `{moved, pending, reason, host_name, to, shell}` |
| `POST /api/pin` | `{host, paglet, minutes?, reason?}` → `{pin, until_ms, host_name, shell}` |
| `POST /api/locate` | `{host, paglet}` → `{host, host_name, moves, shell}` |
| `POST /api/dispose`, `POST /api/terminate` | `{host, paglet, reason?}` → `{done, shell}` |
| `POST /api/zombie-check` | `{host, group}` → `{records, hosts, replicas, takeovers, restarts, accepted, violations, shell}` |
| `POST /api/zombie-cancel`, `POST /api/zombie-redirect` | `{host, group, reason?}`, `{host, group, sink}` → `{state, shell}` |

A launch nonce must be a string of at most 128 bytes. Omitting it or
supplying an empty string generates a fresh nonce. The old `id` field is
refused with HTTP 400: use `nonce` and read the derived ID from `paglet`
in the response. Copied shell commands use `remote launch --nonce`.

The call API defaults to 30000 ms and clamps `timeout_ms` to 100–120000 ms.
Generated shell commands include a nondefault effective timeout as
`--timeout SECONDS`, preserving millisecond precision.

A dispatch answers with `pending:true` while the move has not ended,
including an older host's acknowledgement that it started. Completed moves
and failed moves answer with `pending:false`; `moved` and `reason`
distinguish their outcomes.

Payloads and arguments are JSON values. They become MessagePack for the
paglet, and `{"$bytes": "00ff"}` becomes a binary string (for `bytes`
fields). The shared session client rejects missing or wrong-type protocol
fields rather than inventing default values for a malformed reply. Optional
metadata may be absent on older hosts; unknown extension fields are accepted.
Failures answer with an HTTP status and `{error, shell}`:

- 400 for a malformed request;
- 401 without the token;
- 403 for a refused origin or host;
- 404 for an unknown route;
- 422 for a failed session request, including refusal or a malformed reply;
- 502 when the host could not be reached.
