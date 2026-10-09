# paglets/cpp: the mesh (WP14)

Status: implemented. Companion to the plan (WP14) and to
[cpp-networking.md](cpp-networking.md) (channels, transport, movement) and
[cpp-location.md](cpp-location.md) (liveness). Code:
`cpp/host/src/node/registry.cpp` (the registry and discovery),
`cpp/host/src/net/beacon.cpp` (multicast beacons), the gate in
`cpp/host/src/net/channel.cpp`; tests in `cpp/tests/test_discovery.cpp`,
`cpp/tests/test_network.cpp` and the end-to-end test `host_mesh_discovery`.

## 1. Host announcements

Every host signs an **announcement** of itself: a canonical map `{v: 1,
mesh, key, url, proto, abi: [major, minor], time, relays}`, signed with the host key
over `"paglets host announcement v1" 0x00 SHA-256(body)`. `url` is where the
host can be reached (its advertised address, or its listening address;
empty for a host without an inbound port, which lists its `relays`
instead, planning/cpp-relay.md);
`proto` the mesh protocol and `abi` the paglet ABI it runs. A host makes a
new announcement when its address changes.

A receiver keeps an announcement only if it is for this mesh, from an
enrolled host (the ledger's host list), validly signed, not from the future
(5 minutes of clock skew), and newer than the one it has. Any host can pass
announcements on, but nobody can forge or alter one; a wrong address could
at worst make a host unreachable, since channels prove the key anyway.

## 2. Gossip and bootstrap contacts

- **Frame `hosts`** `{a: [[body, signature, url seen]...]}` on the host
  channels. A host sends its whole registry (its own announcement first) to
  every host it hears from for the first time, to a bootstrap contact that
  answered, and every 2 s to the next host it has an address for
  (anti-entropy). What is new to a host goes on at once to the other live
  hosts (eager push), as ledger records do.
- **Bootstrap contacts**: any enrolled host can introduce a new one. A host
  started with `--join URL` opens a channel to that address without knowing
  the key behind it (`Transport::probe`); the channel proves the key, which
  must be an enrolled host's. The contact then gets the newcomer's registry,
  the newcomer the contact's, and gossip does the rest: the ledger's host
  list replaces fixed seed hosts (`--peer KEY=URL` still works).
- **Addresses** learned from announcements go to the transport; the
  registry is kept in the node's state directory (`hosts`), so a restarted
  host reaches its peers without any contact.

## 3. Multicast beacons

On a local network hosts also find each other without any contact: every
host sends a beacon (UDP, group `239.255.80.71`, port 47471, TTL 1) every
5 s and listens for the others'. A beacon carries the host's signed
announcement and the mesh ID; beacons of other meshes and invalid ones are
ignored. An announcement whose address names no interface (`0.0.0.0`) gets
the address the beacon came from. A host found by a beacon gets this host's
registry at once. `paglets-host serve --no-beacon` turns beacons off,
`--beacon-port` moves them to another port.

## 4. Compatibility gate

- **Mesh protocol** (`net::mesh_protocol`, now 1): both sides announce it in
  the channel handshake (`proto`); a channel between different protocol
  versions is refused on both sides, for hosts and CLI sessions alike.
- **Paglet ABI**: hosts announce `abi: [major, minor]` in the handshake and
  in announcements. Channels between hosts of different major versions are
  refused. The registry marks hosts of another protocol or major version
  incompatible; they are left out of the live view (location records,
  section 2 of cpp-location.md) and out of transfer tickets, and so are
  hosts with an older minor version (a paglet may use newer imports). A
  move to such a host fails with the reason.

## 5. The host registry

`Node::hosts()` lists the enrolled hosts (and only those) with: name,
labels, address, online (heard from within the host timeout; the liveness
of cpp-location.md), last seen, protocol and ABI versions, compatible, and
how the address was learned (`gossip`, `beacon`, `state`). Hosts that leave
the ledger leave the registry. CLI sessions ask for it with `hosts`
(`paglets-host remote hosts`).

## 6. Command line

| `paglets-host serve` option | Meaning |
|---|---|
| `--join URL` | a bootstrap contact (repeatable); tried every 2 s until it answers |
| `--no-beacon` | no multicast beacons |
| `--beacon-port N` | beacons on another UDP port |

`paglets-host remote hosts --connect URL --key KEY --ledger DIR` prints the
registry of that host.

## 7. Exit

The exit of WP14: three hosts discover each other and move paglets by host
name.

| Test | Covers |
|---|---|
| `test_discovery.cpp`: exit | three hosts over HTTPS; b knows only a's address, c only b's, a nobody; they all learn each other's addresses and versions, a paglet moves a to c to b to a by host name, and a host that stops goes offline in the registry |
| beacons | two hosts find each other by multicast beacons alone; forged beacons are ignored |
| gate | a host announcing another ABI major version is incompatible, left out of the live view and of moves (`runs an incompatible paglet ABI`); an announcement not signed by its host is ignored |
| `test_network.cpp` | channels between different protocol versions are refused on both sides; `probe` finds a host by its address |
| `host_mesh_discovery` | three `paglets-host serve` processes with `--join` only: `remote hosts` shows every host online with its address, and a paglet moves a to c to b by name |
