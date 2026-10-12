# paglets/cpp: content exchange between hosts

Status: implemented (follows WP17 and WP20). User documentation:
[System paglets, moving content between hosts](../docs/system-paglets.md#moving-content-between-hosts).

## 1. Problem

A paglet moves as a memory image, and its linear memory holds at most
16 MB. The first File Courier read every file into its memory and wrote it
out on the destination, so it could not carry more than a few MB in all.
The Download Courier carried a downloaded file home the same way. Artifact
capabilities did not help: they named content by hash only, were dropped
on every move (no grant to re-create them from), and could not travel in
messages to other hosts.

## 2. Design

Content stays on disk and moves between hosts; paglets carry capabilities.

- **Movable artifact capabilities.** On a host of a mesh, the resource of
  an `artifact` capability is `<sha256>@<host key ID>/<token>`: the
  content, the host that holds it, and a token that host made.
  `runtime::mesh_artifact` recognises them. They survive moves
  (`Node::Impl::recreate` keeps them, as it keeps pins), travel in messages
  and replies to paglets on other hosts (`portable_caps`,
  `deliver_remote`), and go with clones. Capabilities without a host part
  (hosts without a node, older stores) behave as before.
- **Tokens without state.** The token is the first 16 bytes of
  SHA-256(Ed25519 signature of the host over
  `"paglets content token\n" + hash`). Only the host can make it, it
  needs no table, and it survives restarts. A token does not expire; it
  gives exactly the content it names.
- **`files.share`** hashes a file of a directory capability (right `read`)
  on a job thread and remembers it in `shares.state` (path, size,
  modification time, media type, the root's mark). It replies with a
  movable artifact capability. Nothing is copied and nothing is read into
  the paglet, so the paglet is not marked. The file is served while its
  size and modification time stay.
- **`files.save`** writes the content of a lent artifact capability (second
  lent capability) below a directory capability (`create`, `write` to
  replace). Content of this host is copied; otherwise the host pulls it
  from the holding host straight into a temporary file beside the target,
  and renames it after the hash check.
- **`artifacts.get` and `stat`** on a host without the content pull it into
  the local store first (within `artifacts_limit`), then answer. Concurrent
  requests for the same content share one pull.

## 3. Protocol

Node frames (`host/src/node/exchange.cpp`), over the existing end-to-end
channels between hosts, so relays and hosts without inbound ports work
unchanged:

| Frame | Fields | Meaning |
|---|---|---|
| `content-want` | `r` request, `h` hash, `k` token, `o` offset | asks for the part at `o` |
| `content-part` | `r`, `o`, `n` total size, `d` up to 1 MB, `s` marks (first part only) | one part |
| `content-missing` | `r`, `e` reason | refused or not there |

The pulling host asks for the first part, learns the size, and then keeps
at most eight parts (8 MB) outstanding, which stays well inside a relay's
downlink buffer. Parts must arrive in order (the transport keeps order per
peer); the file is written and hashed as they arrive. A part out of order,
a size above the limit, a hash mismatch or 30 s without progress ends the
pull, and the partial file is removed.

Throughput (measured with `cpp/tools/mesh_bench.sh` on a gigabit LAN) came
from four changes to the transport and the hosts:

- `TCP_NODELAY` on the HTTPS connections between hosts. Without it, Nagle's
  algorithm and delayed acknowledgements held small frames (a
  `content-want`, a message between paglets) for about 40 ms: round trips
  between paglets took 45 to 95 ms on a LAN, and now about 1 to 4 ms.
- Frames of hosts are handled on a thread of the receiving transport, in
  order, after the request that carried them is answered. Before, the
  receiver wrote and hashed every part inside the request, and the sender
  waited for that before it sent its next batch.
- SHA-256 of content in the host through OpenSSL, which uses the CPU's SHA
  instructions (`ContentHash`); the portable implementation that guests
  share hashed 256 MB in 2.5 to 10 s, OpenSSL in 0.2 s.
- TLS between hosts prefers ChaCha20-Poly1305, which the Raspberry Pi 4
  (no AES instructions) encrypts several times faster than AES-GCM.

The holding host checks every `content-want`:

1. the asking host is enrolled;
2. the token is the one it makes for that hash;
3. it holds the content (a stored artifact, or a shared file that has not
   changed);
4. data residency: for every `root:<name>` mark of the content, a
   `host-only` rule refuses, and a `hosts` rule must select the asking host.

Refusals by residency or token become `denied` for the paglet, others
`not_found`, and timeouts `timeout`.

## 4. Data residency

Residency used to be enforced only when a marked paglet moved. Content that
moves without the paglet is now checked where it leaves: the holding host
applies the same rules to the asking host. A paglet that reads content with
`artifacts.get` takes its marks, as before. Listing or finding files still
marks the paglet, since file names are content of the root.

## 5. Not yet

- No revocation of a token short of changing or unsharing the content;
  shares are forgotten only when more than 10 000 are remembered.
- The artifact store has no garbage collection; fetched content stays
  until the store is cleared.
- A pull asks only the host named in the capability, not other hosts that
  may hold a copy.
