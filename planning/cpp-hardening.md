# paglets/cpp: security hardening (WP21)

Status: in progress. Fuzzing of the decoders runs in CI; the external
review of the cryptographic protocol and the per-platform sandbox review
need people outside this repository. Companion to the plan (WP21) and to
the threat notes of the security design (section 10 of
[cpp-security-and-communication.md](cpp-security-and-communication.md)).

## 1. Fuzzing

`cpp/tests/fuzz.hpp` is a small mutation fuzzer that works with GCC (no
libFuzzer): each target starts from valid inputs and runs on mutations of
them (bit flips, interesting bytes, insertions, deletions, duplicated
ranges, splices with other inputs, length-like 32-bit values), keeping some
mutants as new starting points. Targets must neither crash nor trip a
sanitizer; some also check properties.

| Target | Decoder | Property |
|---|---|---|
| `mesh-value` | msgpack reader, mesh values | what decodes encodes to the same value |
| `records` | ledger records, passports, ledger derivation with a damaged record | |
| `mobility` | travelling state, capabilities, remote messages | |
| `wasm-module` | the Wasm module parser | |
| `memory-image` | memory images (deserialize and restore into a module) | |
| `contracts` | contracts through reflection (mesh-info, web, ai, files), ABI documents | |
| `noise-message-1`, `noise-message-2` | the Noise XX handshake, both directions | |
| `json`, `url`, `html` | JSON, URLs, HTML text extraction | JSON and resolved URLs are stable |
| `node-frames` | every frame a host accepts from another (gossip, mesh-info, moves, location) | the host still moves paglets afterwards |

The tests are `fuzz:` in the unit tests: two seconds per target in CI, under
ASan+UBSan and TSan as well. Longer runs:

```bash
PAGLETS_FUZZ_SECONDS=600 build/linux-gcc16-asan/tests/paglets_tests guests "fuzz:"
```

`PAGLETS_FUZZ_SEED` repeats a run; the input that crashes a target is saved
as `fuzz-crash-<target>.bin`, and `PAGLETS_FUZZ_REPLAY=<file>` runs a target
on it alone.

## 2. Limits

One owner's paglets must not take a host over. The runtime's limits
(`paglets::runtime::Config`), tested by `hardening:` in the unit tests:

| Limit | Default | Beyond it |
|---|---|---|
| `owner_paglet_limit` | 10 000 paglets of one owner per host (system paglets do not count) | `create`, children and clones get `quota`; a paglet arriving from another host is refused and stays where it was (`on_move_failed` with the reason) |
| `spawn_limit_per_call` | 16 children and clones per handler call | `quota` |
| `pin_limit` | 64 pins held by one paglet (expired pins do not count) | the pin is refused (`quota`) |
| pin duration | 1 s to 30 days, at most the policy rule's maximum (1 hour without one) | shortened |
| `mailbox_limit`, `timer_limit`, `cap_limit` | 1 024 messages, 64 timers, 1 024 capabilities per paglet | `quota` |
| `handler_budget` | 5 s per handler | the handler is ended |
| storage quota | 16 MB per paglet | `quota` |
| message size | 1 MB | `too_large` |

A clone bomb (a paglet whose every instance clones itself) stops at the
owner's limit; the host keeps serving other owners' paglets, and the owner
can run new paglets as soon as some of the bomb's end.

## 3. Still to do

- Ledger partition tests beyond those of WP8.
- Key handling review (storage, agent, rotation) and the per-platform worker
  sandbox review.
- External review of the cryptographic protocol (Noise XX channels, record
  and passport signatures).
- Threat model reviewed against the implementation.
