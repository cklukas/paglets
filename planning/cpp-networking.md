# paglets/cpp: networking and movement (WP12)

Status: implemented (WP12). Companion to the plan (sections 3.1, 3.2 and 3.7) and to
[cpp-security-and-communication.md](cpp-security-and-communication.md)
(sections 3.1, 4.6 and 5.1), which state the goals; this document records the
concrete design.

## 1. Layers

1. **Transport**: HTTPS (TLS 1.3). It keeps reverse proxies and firewalls
   working; it is not what authenticates hosts.
2. **Channels**: end-to-end encrypted, mutually authenticated Noise channels
   between identity keys of a mesh (section 2), carried over the transport.
3. **Frames**: canonical values (the ledger's encoding) inside channels:
   gossip, module transfers and launches (WP11), deliveries and moves.

## 2. Channels

The Noise protocol framework (revision 34) with the pattern XX:
`Noise_XX_25519_ChaChaPoly_SHA256`, built on libsodium (X25519,
ChaCha20-Poly1305 IETF, SHA-256 and HMAC-SHA256). Code:
`cpp/host/src/net/noise.cpp` and `channel.cpp`; tests in
`cpp/tests/test_noise.cpp` include the test vector of the cacophony suite.

```text
-> e
<- e, ee, s, es
-> s, se
```

- **Static keys** are the X25519 forms of the Ed25519 identity keys (host
  keys between hosts; admin or owner keys for CLI sessions). Both sides prove
  possession of them in the handshake; ephemeral keys give forward secrecy.
- **Identities**: the responder's second message and the initiator's third
  message carry a payload `{v: 1, key: <Ed25519 public key>, role: host |
  admin | owner, url?}`, encrypted by the handshake (`url`: where a host can
  be reached, section 3). The receiver converts the key
  to X25519 and requires it to equal the static key the handshake proved, so
  the peer of a channel is an identity key; the caller then checks it
  against the ledger (an enrolled host, an admin, an enrolled owner).
- **Prologue**: `"paglets channel v1" 0x00 <mesh ID>`. Peers of different
  meshes derive different keys and fail at the second message.
- **Frames**: a frame is split into Noise messages of at most 65 535 bytes;
  each plaintext starts with a flag byte (1: last part). Receivers refuse
  frames over their limit (64 MB by default). A message that fails
  authentication, arrives twice or out of order breaks the channel; the
  sender then opens a new one.
- The handshake hash is kept as the channel binding.

## 3. Transport

Every host runs an HTTPS server (cpp-httplib with OpenSSL 3; code in
`cpp/host/src/net/transport.cpp`, tests in `cpp/tests/test_network.cpp`). A
host sends frames to a peer through a channel it opened to the peer's
server; each direction uses the channel of its sender:

| Request | Body | Answer |
|---|---|---|
| `POST /paglets/v1/open` | Noise message 1 | `{s: session ID, m: Noise message 2}` |
| `POST /paglets/v1/finish/<session>` | Noise message 3 | 204, or 403 if the peer is refused |
| `POST /paglets/v1/frames/<session>` | Noise messages | Noise messages (answers of CLI sessions) |

- Frame bodies are sequences of `[u32 big-endian length][Noise message]`.
  A server handles the requests of a session one at a time, so frames arrive
  in the order they were sent. Sessions end after 10 minutes without use;
  a request for an unknown session (404) was not processed, so the sender
  opens a new channel and sends again.
- **Senders**: every peer has an ordered queue and a sender thread that
  batches frames (8 MB per request, larger frames alone). Frames that cannot
  be delivered (no address, the peer refuses the channel or answers with
  another key, transport errors after the request went out) are dropped and
  counted; the protocols above time out and retry.
- **Addresses**: `https://host:port`, configured per peer. A host announces
  its own address in its channel's handshake payload (`url`); since the
  channel proves the key, a host can only announce an address for itself.
  A host that knows only a seed thereby becomes reachable to it. Discovery
  (WP14) will add addresses from gossip.
- **Acceptance**: a host accepts channels from enrolled hosts and its seeds
  (host role), and CLI sessions of admins and enrolled owners (section 8).
- **TLS**: keeps reverse proxies and firewalls working, but does not
  authenticate hosts (the channels do). A host uses a certificate and key
  from PEM files, or makes a self-signed one (P-256) at start. Peers'
  certificates are checked only against a configured CA file.
- **CLI sessions** (`ClientSession`): an admin or owner key opens a channel
  to a host and exchanges request and answer frames in one HTTP request each.

## 4. Messages across hosts (runtime)

The runtime's side is `cpp/host/include/paglets/runtime/mobility.hpp`;
tests in `cpp/tests/test_mobility.cpp` join two runtimes directly.

- **Host hints**: endpoint and reply capabilities whose target is on
  another host carry that host's key ID (`Cap::host`). A capability that
  leaves a host (in a message, or with a moving paglet) gets the ID of the
  host its target is on; endpoints to system paglets get none, since every
  host has its own.
- **Remote messages**: sends, requests and replies to paglets elsewhere
  leave through the mesh's delivery hook as `RemoteMessage`s (target, host,
  name, payload, priority, the host-stamped sender record, badge, endpoint
  capabilities; for requests the requester, its host, the correlation and
  the timeout; for replies the correlation and status) and arrive through
  `Runtime::deliver_remote`. Requests time out on the requester's host;
  requests that cannot be delivered (unknown paglet, full mailbox, a system
  paglet) are answered with the error. Only endpoints to paglets travel with
  messages; lending needs a system paglet on the same host.
- **Tombstones**: a host remembers for 10 minutes (`Config::tombstone_ttl`)
  where a departed paglet went and forwards messages and replies for it
  (at most 4 times per message), so endpoints with stale hints keep
  working until the location service (WP13) takes over.

## 5. Departures and arrivals (runtime)

- A **departure** (`Departure`): after the handler that called `dispatch`
  the paglet receives `dispatching`, its image is taken, and the runtime
  hands its travelling state (`TravelState`: ID, module, owner, handle and
  correlation counters, services, every capability with host hints,
  requests in flight with their remaining time) and image to the mesh. The
  paglet waits inactive; messages to it are held.
- The mesh reports the outcome with `finish_departure`: on success the
  held messages follow the paglet, a tombstone records where it went, its
  stored record and host directories here are removed, and system paglets
  hear that it ended here. On failure it continues here with its state and
  receives `paglets.move_failed`.
- An **arrival** is two-phase: `prepare_arrival` creates the paglet without
  running it (module present, admission check, handles preserved, timers
  and request timeouts re-armed, capabilities re-created or listed as
  lost); `commit_arrival` stores it at once and starts it with `arrived`;
  `abort_arrival` discards it. The mesh commits on the destination only
  when the source has agreed (section 6), so a paglet never runs twice.
- A **clone for another host** leaves as a departure with the clone's ID,
  the original's transferable endpoints and service handles, and the
  endpoints passed to it; on success endpoints to it name its host.
- Only roaming paglets move; arriving paglets are always roaming.

## 6. Moving paglets (mesh)

The node's side (`cpp/host/src/node/movement.cpp`; tests in
`cpp/tests/test_movement.cpp`, over the in-memory network and over HTTPS).

**Transfer tickets.** A destination is a ticket:
`<target>[?retries=N&arrival=active|inactive]`, where the target is a host
name, a key ID (or a prefix of at least 8 digits), `label:<label>` or
`any`. The source picks up to `retries` (default 3) enrolled hosts other
than itself that match, and skips hosts where an item of the paglet's
manifest (in its passport) is denied and no grant of the paglet covers it
(preflight, cpp-policy.md, section 6). It offers the paglet to one host
after the other until one takes it; `arrival=inactive` stores it on
arrival without running it until a message comes. Paglets need a
passport to move (paglets of enrolled owners).

**Frames** (canonical values on the host channels):

| Frame | Members | Meaning |
|---|---|---|
| `move-offer` | `r`, `b` (body), `s` (signature) | the signed envelope (below) |
| `move-want` | `r`, `p` [page index] | pages the destination lacks |
| `move-pages` | `r`, `p` [[index, zstd bytes]] | pages, at most 1 MB compressed per frame |
| `move-ready` | `r` | prepared: waiting for the decision |
| `move-refused` | `r`, `e` | not taken, and why |
| `move-commit`, `move-abort` | `r` | the source's decision |
| `move-query` | `r` | a prepared destination asks for the decision |
| `deliver` | `m` (a remote message) | messages, requests and replies across hosts (section 4) |

**The signed envelope.** The offer's body is a canonical map: version,
mesh, source and destination host keys, request ID, kind (`dispatch` or
`clone`), paglet ID, passport, travelling state (section 5), page manifest
(`page_count`, globals, a SHA-256 hash or nil per 64 KB page), the arrival
mode, time, and for clones the arguments, capabilities and original. The
source host signs `"paglets move offer v1" 0x00 SHA-256(body)` with its
host key, so the destination can check who sent it independent of the
channel it came through (relays, WP15).

**The destination** refuses before any page moves unless: the offer comes
from an enrolled host and verifies; it names this host and mesh; the
passport verifies for the paglet and module (owner enrolled, not expired;
clones and children carry links signed by enrolled hosts); the module may
run as a roaming paglet (cpp-modules.md, section 4); the paglet is not here
already. It then gets the module (cpp-modules.md, section 5, asking the
source first) and asks only for pages it does not have: pages of images a
host sent or received stay in a page cache (256 MB, least recently used out),
so repeated moves between hosts transfer only changed pages. Every page is
checked against the manifest's hash. With all pages it prepares the arrival
(capabilities from grants are re-created, section 7) and answers
`move-ready`.

**Commit.** The source decides: on `move-ready` it records the commit
(durably, in the node's state) and only then tells the runtime (held
messages follow the paglet, section 5) and the destination (`move-commit`),
which starts the paglet and stores it and its passport. A refusal, or no
answer within the move timeout (30 s per step), moves on to the next host of
the ticket (the destination is told `move-abort`); when none is left the
paglet stays with `paglets.move_failed`. A prepared destination that hears
nothing asks with `move-query` until the source answers; it never decides
itself, so a paglet neither runs twice nor gets lost.

**Passports.** A moving paglet's passport goes with it (and is removed at
the source). Clones for another host get a link signed by the source host;
children and local clones get links when they are made (the runtime reports
them, `Runtime::take_spawns`), so they can move as well.

## 7. Capabilities on arrival

As section 4.6 of the security design says, grants follow the paglet;
host-local handles do not:

| Capability | On arrival |
|---|---|
| Endpoint to a paglet | Kept, with the host hint of its target |
| Default service endpoint | The destination's service endpoint under the same handle |
| Resource capability (`dir`, ...) from a grant | Re-created from the grant if it is the paglet's own, valid, and its host selector covers the destination (the named root must exist there); narrowings the paglet made (fewer rights, a subdirectory, an earlier expiry) stay |
| Endpoint to a service with ambient authority, from a grant | Re-created the same way |
| Reply capability | Kept |
| Timer | Re-armed for its time |
| Anything else | Lost: listed in the `arrived` event |

## 8. Hosts on the network and CLI sessions

`paglets-host serve` runs a host of a mesh (code in
`cpp/host/src/serve_cli.cpp`): it opens the host key and the host's copy of
the ledger, runs the runtime with the system paglets and the node, and
serves channels over HTTPS until SIGINT, SIGTERM or a stop file.

| Option | Meaning |
|---|---|
| `--key FILE`, `--ledger DIR`, `--state DIR` | host key, ledger copy, state (runtime, services, node) |
| `--listen HOST:PORT` | address of the HTTPS server (default `127.0.0.1:0`, a free port) |
| `--advertise URL` | the address announced to peers (default: the listening address) |
| `--peer KEY-ID=URL` | a seed host and its address (repeatable) |
| `--root NAME=DIR` | a named root of the files service (repeatable) |
| `--module-source DIR` | a directory of modules the host may load (repeatable) |
| `--tls-cert`, `--tls-key`, `--tls-ca` | PEM certificate and key (default: self-signed), CA for peers |
| `--threads N`, `--in-process`, `--no-sandbox`, `--stop-file FILE` | runtime lanes, workers, stopping |

It prints its key ID, the mesh ID and its URL. Host channels carry the
frames of sections 6 and 7 and of WP11; channels of admins and owners are
**CLI sessions**: each request frame gets one answer frame, a canonical map
`{ok: true, ...}` or `{ok: false, e: <reason>}` (`Node::answer_session`,
`cpp/host/src/node/session.cpp`).

| Request (`t`) | Members | Who | Answer |
|---|---|---|---|
| `status` | | admins, owners | host key and name, mesh, ledger digest and record count, paglets (ID, module, owner, state) |
| `push` | `records` | admins, owners | records added; errors. The ledger checks each record's signatures |
| `launch` | `passport`, `module`?, `args`? | the passport's owner | the new paglet's ID; the module is added only if its hash matches the passport |
| `call` | `paglet`, `name`, `payload`, `timeout_ms`? | the paglet's owner, admins | the reply's status and payload |
| `dispatch` | `paglet`, `destination` | the paglet's owner, admins | the move started (a transfer ticket, section 6) |

The client side is the `remote` command group of `paglets-host`
(`status`, `push`, `launch`, `call`, `dispatch`, all with `--connect URL
--key KEY --ledger DIR`). The session's role comes from the key file (admin
or owner); the client accepts the server only if it is a host enrolled in
its ledger copy, or the key given with `--host-key`. `launch` signs the
passport with the owner key on the client and sends the module with it.

## 9. Exit

The exit of WP12: dispatch and clone between two hosts; repeated moves
transfer only changed pages; grants follow the paglet; failed transfers
leave the paglet on the source.

| Test | Covers |
|---|---|
| `test_noise.cpp` | the Noise test vector, identities, tampering, replays, other meshes |
| `test_network.cpp` | ordered frames over HTTPS, refused strangers, restarted hosts, CLI sessions, self-signed certificates |
| `test_mobility.cpp` | ABI C37 to C40 between two runtimes: dispatch, held messages, failed moves, clones, remote requests |
| `test_movement.cpp` | the exit over the in-memory network (dispatch there and back, page reuse, clones, grants, failed transfers, tickets, lost commits) and over HTTPS |
| `host_serve_move` (`cpp/tests/cli_serve.cmake`) | two `paglets-host serve` processes; an owner launches a paglet on one with `remote launch`, dispatches it to the other (which fetches the module) and an admin dispatches it back; its state follows |
