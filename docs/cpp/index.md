# paglets/cpp

paglets/cpp is the C++ edition of paglets. Its paglets are WebAssembly
modules, run by the embedded
[WAMR](https://github.com/bytecodealliance/wasm-micro-runtime) runtime.
They move between hosts as **memory images**: the paglet's whole state is
ordinary C++ objects in its linear memory, and a move sends that memory
(only the pages that changed since the target last saw it), not
serialized fields.

> [!NOTE]
> paglets/cpp lives on the `cpp` branch, in the `cpp/` directory of the
> repository. It is a separate implementation from the Python package
> described in the rest of this site: the two editions do not exchange
> paglets. The design and the work packages are in the
> [planning documents](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-edition-plan.md).

## What it offers

- **Code mobility.** Modules travel with the paglet. A host that lacks a
  module fetches it from its sources or from other hosts, verified by hash.
  Hosts do not need the same code installed.
- **Sandboxing.** Paglets run in worker processes that the operating system
  sandboxes: seccomp on Linux, a sandbox profile on macOS, and a job object
  with a restricted token on Windows. Paglets cannot open files, create
  sockets or start programs. They reach host resources only through
  **system paglets**, within the rules of the mesh policy.
- **A secured mesh.** Hosts, admins, owners and signers have keys. A
  signed ledger, spread by gossip, records who belongs to the mesh, which
  modules are trusted, which policy rules apply and where data may go.
  Hosts talk over HTTPS, inside end-to-end encrypted, mutually
  authenticated Noise channels. Hosts behind NAT are reached through
  relays.
- **Movement with guarantees.** Paglets move with their grants and
  capabilities. Transfer tickets name the destination by host, label,
  service offer or `any`. Location records find a paglet anywhere, and
  pins keep it in place for a while. A paglet that reads a restricted root
  carries that root's residency mark and moves only where its data may go.
- **Gateways to the web and to AI.** Hosts can offer mediated web access
  (`web`) and local inference (`ai`). Paglets find these offers through
  `mesh-info` and move to them.

## Pages

| Page | Contents |
|---|---|
| [Building and testing](building.md) | toolchains, presets, build options, guest modules, tests and sanitizers |
| [Writing paglets](writing-paglets.md) | the guest SDK, messages, capabilities, services and the patterns library |
| [Guest SDK reference](sdk-reference.md) | every class, function and lifecycle hook of `<paglets/paglet.hpp>`, and your own typed service contracts |
| [Hosts and meshes](hosts-and-meshes.md) | running hosts, the ledger, policy, module trust, residency, TLS, the remote CLI |
| [Command-line reference](cli-reference.md) | every command and option of `paglets-host`, `paglets-worker` and `paglets-spike` |
| [Configuration and troubleshooting](configuration.md) | state directories, keys and passphrases, ports and firewalls, limits, error codes, common problems |
| [System paglets](system-paglets.md) | the services hosts offer to paglets, and the web and AI gateways |
| [Demos and benchmarks](demos.md) | the demo paglets, what each shows, and the first benchmark numbers |

## Wire formats and interoperability

paglets/cpp shares concepts with the Python edition (hosts, paglets,
messages, a mesh, services, compute slots), but none of its formats. A
Python host and a C++ host cannot talk to each other, and a paglet of one
edition cannot run on the other.

| Layer | paglets/cpp | Python edition |
|---|---|---|
| Transport | HTTPS; every host runs an HTTPS server with the endpoints `/paglets/v1/open`, `/paglets/v1/finish/<session>`, `/paglets/v1/frames/<session>` and `/paglets/v1/poll/<session>` | HTTP with a JSON control API, HTTPS through a reverse proxy |
| Authentication | a Noise channel inside HTTPS: `Noise_XX_25519_ChaChaPoly_SHA256` between the Ed25519 keys of hosts, admins and owners, bound to the mesh ID | an optional shared API key (`PAGLETS_API_KEY`) |
| Framing | request bodies are sequences of `[u32 big-endian length][Noise message]`; a frame is split into Noise messages of at most 65 535 bytes | JSON bodies, chunked binary payloads for movement |
| Frames between hosts | canonical MessagePack values (sorted string keys, shortest encodings, no floats), the same encoding as ledger records | JSON control calls |
| Messages to paglets | MessagePack payloads; the CLI converts JSON to MessagePack and back | Python values |
| Moving a paglet | a signed move offer with a page manifest, then the missing 64 KB pages of the memory image, compressed with zstd | pickled dataclass state, streamed over HTTP |
| Paglet code | WebAssembly modules (paglet ABI v1), fetched by SHA-256 hash | Python classes importable on every host |

Channels also check the mesh protocol version (`paglets-host --info`
prints it) and the paglet ABI. Hosts refuse peers of another mesh, another
protocol version or another ABI.

The full protocol is in the planning documents:

- [networking and movement](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-networking.md)
  (channels, transport, frames, moves, CLI sessions);
- [mesh discovery](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-mesh.md)
  (announcements, gossip, beacons, the compatibility gate);
- [security and communication](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-security-and-communication.md);
- [paglet ABI v1](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-abi-v1.md)
  (the interface between host and guest);
- [ledger](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-ledger.md)
  (records and their canonical encoding).

## Status

Milestones M0 (feasibility), M1 (single-host runtime), M2 (mesh identity,
policy and system paglets) and M3 (movement between hosts) are closed.
Milestone M4 (applications) is in progress. Its parts so far:

- `mesh-info` and `compute-slots`, with the pi example;
- service offers, the gateways and data residency;
- the patterns library;
- admin work against live hosts from the CLI;
- the demo paglets and benchmarks;
- fuzzing of every decoder that reads data from outside;
- this documentation.

The
[results of M0](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-m0-results.md),
[M1](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-m1-results.md) and
[M2](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-m2-results.md)
record the measurements and decisions of those milestones. M3 has no
results document of its own. Its parts are described in the designs of
[code mobility](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-modules.md),
[networking](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-networking.md),
[location](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-location.md),
[the mesh](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-mesh.md) and
[relays](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-relay.md).
The [plan](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-edition-plan.md)
lists every work package.
