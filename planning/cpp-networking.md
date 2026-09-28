# paglets/cpp: networking and movement (WP12)

Status: in progress. Companion to the plan (sections 3.1, 3.2 and 3.7) and to
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
  (host role), and CLI sessions of admins and enrolled owners (section 6).
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
