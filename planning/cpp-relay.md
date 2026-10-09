# paglets/cpp: relaying for hosts without inbound ports (WP15)

Status: implemented. Companion to the plan (WP15),
[cpp-networking.md](cpp-networking.md) (channels and transport) and
[cpp-mesh.md](cpp-mesh.md) (announcements and the registry). Code:
`cpp/host/src/net/transport.cpp` (downlinks, tunnels, uplinks) and
`cpp/host/src/node/registry.cpp` (choosing relays); tests in
`cpp/tests/test_discovery.cpp` and the end-to-end test
`host_mesh_discovery`.

## 1. The problem

A host behind NAT or a firewall can open connections, but nobody can open
one to it. The transport sends frames to a host by opening an HTTPS
connection to its address, so such a host could send but never receive.
No host is special: every host that can be reached relays for others as a
normal duty, and relayed frames stay end-to-end encrypted.

## 2. Downlinks: polling the relays

A host without an inbound port (`TransportConfig::listen = false`,
`paglets-host serve --no-listen`) keeps a few **relays**: hosts it can
reach. For each it keeps a channel of its own and asks
`POST /paglets/v1/poll/<session>`; the relay answers with the frames it
holds for the host, or waits up to 10 s (`poll_wait`) for some. The
answer is sealed with the poll channel, so only the host can read it.

A relay holds the frames for a host that polls it (up to 64 MB,
`max_downlink_bytes`; more are dropped) while the host has polled within
the poll time plus the transport timeout. A host's sender for a peer
without an address first uses such a downlink.

## 3. Tunnels: end-to-end channels through a relay

Every other host reaches a host without an inbound port through one of its
relays, over a Noise channel of its own to that host (the same handshake
and keys as direct channels, section 2 of cpp-networking.md), carried in
**transport control frames**. Control frames start with the byte `0xc1`,
which no canonical value starts with, and never reach the node:

| Control frame | Members | Meaning |
|---|---|---|
| `fwd` | `to`, `d` | to a relay: pass `d` on to host `to` |
| `via` | `from`, `d` | from a relay: `d` comes from host `from` (the relay's channel proved who sent it) |

`d` is a tunnel message `{t, s, m}` with `s` the tunnel's session:

| `t` | `m` | Meaning |
|---|---|---|
| `open`, `open2`, `finish` | one Noise handshake message | the three handshake messages; the responder requires the proven key to be `from` and accepts it like any channel |
| `data` | Noise messages | frames, decrypted only by the end host |
| `reset` | | the tunnel is unknown or broken: the sender opens a new one |

The relay sees only the two host keys and the sizes. A host whose frames
to a relay are dropped is avoided for 30 s; tunnels through it are opened
again through the next relay of the peer. Two hosts without inbound ports
reach each other the same way (each through the other's relays).

## 4. Choosing relays

- A host without an inbound port keeps 2 relays (`DiscoveryTiming::relays`):
  hosts with an address that are up and compatible, those heard from most
  recently first. A relay whose polls fail for 10 s
  (`DiscoveryTiming::relay_timeout`) is replaced.
- Its announcement (cpp-mesh.md, section 1) carries no address and the
  relays (`relays`: their keys, at most 8); a new announcement goes out
  when they change. Hosts that learn it tell their transport
  (`set_relays`). The registry and `paglets-host remote hosts` show them.
- The bootstrap contact (`--join`) is the first relay, so a newcomer
  receives the registry through it.

## 5. Exit

The exit of WP15: a host behind NAT participates in the mesh, and keeps
participating when one of its relaying peers goes down.

| Test | Covers |
|---|---|
| `test_discovery.cpp`: exit | hosts a, b, c can be reached, d cannot; d keeps two relays, every host reaches it through them, ledger records reach it, a paglet moves to d and on by host name; then one of d's relays goes down: d takes another, the others follow, and the paglet moves to d and back again |
| two hosts without inbound ports | c and d (no inbound port) exchange a paglet through tunnels via a and b |
| `host_mesh_discovery` | a fourth `paglets-host serve --no-listen` joins through c; every host lists it with its relays, and a paglet moves to it by name |
