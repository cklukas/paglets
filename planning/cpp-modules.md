# paglets/cpp: module store and code mobility (WP11)

Status: in progress. The module store, the cache of compiled modules and
garbage collection are implemented in `cpp/host/src/runtime_modules.cpp`
(`paglets::runtime::ModuleStore`) and tested in `cpp/tests/test_modules.cpp`.
Companion to the plan (section 3.5) and to
[cpp-security-and-communication.md](cpp-security-and-communication.md)
(sections 3 and 6).

## 1. Module store

- Modules are stored by the lowercase hex form of their SHA-256 hash. With a
  state directory the store is `modules/` in it: `<hash>.wasm` and
  `<hash>.meta` (MessagePack: `added`, `last_used`, `pinned`; Unix
  milliseconds). Without one, modules are kept in memory.
- Adding a module checks it once: valid Wasm, the paglet ABI of a system
  paglet (version marker, exports, snapshot readiness) and the widest import
  set. The trust class of a paglet is checked when it is created.
  `add_verified` also requires the bytes to hash to the hash that was asked
  for; modules from other hosts and sources go through it.
- The hash is checked again whenever a module is read from disk: when the
  store opens (damaged files are reported and left out) and before a module
  is compiled or handed to another host.
- Files are written atomically (temporary file, then rename); leftover
  temporary files are removed when the store opens.

## 2. Compiled modules

WAMR's fast interpreter translates a module into its own code when it loads
it; that translation is the compiled form. It depends on the engine build and
lives in memory, so it is never stored or sent to other hosts.

- `acquire(hash)` returns the compiled module, shared by every instance of
  it in the host process. The runtime acquires the module when it places a
  paglet and releases it with the instance, so inactive paglets hold no
  compiled code.
- A least-recently-used cache keeps compiled modules without users up to
  `Config::module_cache_bytes` (default 64 MB, measured by module size).
  Modules in use stay loaded regardless of the budget.
- Worker processes compile their own copies. After a lane releases
  instances, its worker drops the modules that neither a paglet placed on
  the lane nor the host's cache keeps (IPC operation `unload_module`).

Open: AOT compilation (`wamrc`, which needs LLVM) would give a persistent
compiled form; its cache would be a directory keyed by module hash, WAMR
version and CPU architecture, as the plan says. The fast interpreter stays
the portable default (M0 results, finding 5).

## 3. Use counts and garbage collection

- Every paglet on the host, active or not and including recovered ones,
  counts as a user of its module. A module without users keeps the time its
  last user ended (or it was added or acquired) as its last use.
- `collect(grace)` removes modules that have no users, are not pinned and
  were not used within the grace period: file, metadata and compiled form.
  A module being collected cannot gain a user at the same time; creating a
  paglet from it fails with "unknown module" afterwards.
- The runtime collects every `Config::module_gc_interval` (default 10
  minutes) with `Config::module_gc_grace` (default 24 hours), and logs what
  it removed. Pinned modules (for example modules a host keeps for
  arrivals) are never collected.
