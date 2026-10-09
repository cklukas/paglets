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

## 2. Still to do

- Quota, pin and clone-bomb tests; ledger partition tests beyond those of
  WP8.
- Key handling review (storage, agent, rotation) and the per-platform worker
  sandbox review.
- External review of the cryptographic protocol (Noise XX channels, record
  and passport signatures).
- Threat model reviewed against the implementation.
