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
| `--module-dir DIR` | none | `.wasm` files offered for launching and pushing |
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
  with a priority;
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
module's `launch` type), an optional paglet ID and how long the passport
is valid. **Push only** stores a module file on a host without starting
it.

Every page refreshes itself without losing what you typed or which
sections you opened.

> [!NOTE]
> Forms need an [interface description](interface-descriptions.md) in the
> module. Paglets without one still get *Other message* (name and JSON),
> and modules without one are launched with JSON arguments.

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

`paglets-web` acts with your key, so it protects its API from other
programs and websites:

- it listens on `127.0.0.1` only, unless `--listen` and `--allow-remote`
  say otherwise;
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
| `POST /api/call` | `{host, paglet, name, payload?, timeout_ms?}` → `{status, status_name, reply, shell}` |
| `POST /api/send` | `{host, paglet, name, payload?, priority?}` → `{sent, shell}` |
| `POST /api/launch` | `{host, module (hash) or file (in --module-dir), args?, id?, hours?}` → `{paglet, host, shell}` |
| `POST /api/push-module` | `{host, file}` → `{hash, shell}` |
| `POST /api/dispatch` | `{host, paglet, destination}` → `{moved, pending, reason, host_name, to, shell}` |
| `POST /api/pin` | `{host, paglet, minutes?, reason?}` → `{pin, until_ms, host_name, shell}` |
| `POST /api/locate` | `{host, paglet}` → `{host, host_name, moves, shell}` |
| `POST /api/dispose`, `POST /api/terminate` | `{host, paglet, reason?}` → `{done, shell}` |

Payloads and arguments are JSON values. They become MessagePack for the
paglet, and `{"$bytes": "00ff"}` becomes a binary string (for `bytes`
fields). Failures answer with an HTTP status and `{error, shell}`:

- 400 for a malformed request;
- 401 without the token;
- 403 for a refused origin or host;
- 404 for an unknown route;
- 422 when the host refused;
- 502 when the host could not be reached.
