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
| [Building and testing](building.md) | toolchains, presets, build options, tests and sanitizers |
| [Writing paglets](writing-paglets.md) | the guest SDK, messages, capabilities, services and the patterns library |
| [Hosts and meshes](hosts-and-meshes.md) | running hosts, the ledger, policy, module trust, residency, the remote CLI |
| [System paglets](system-paglets.md) | the services hosts offer to paglets, and the web and AI gateways |
| [Demos and benchmarks](demos.md) | the demo paglets, what each shows, and the first benchmark numbers |

## Status

Milestones M0 (feasibility), M1 (single-host runtime), M2 (mesh identity,
policy and system paglets) and M3 (movement between hosts) are closed.
Milestone M4 (applications) is in progress. Its parts so far:

- `mesh-info` and `compute-slots`, with the pi example;
- service offers, the gateways and data residency;
- the patterns library;
- admin work against live hosts from the CLI;
- the demo paglets and benchmarks;
- fuzzing of every decoder that reads data from outside.

The
[results of M0](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-m0-results.md),
[M1](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-m1-results.md) and
[M2](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-m2-results.md)
record the measurements and decisions of the closed milestones.
