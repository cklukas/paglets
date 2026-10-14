# paglets/cpp: the paper, and the refinement plan behind it

Status: in progress (started 2026-10-14) on the branch `cpp-paper`. The
paper lives in `papers/paglets-cpp/`; this document is its plan, and the
list of everything that writing it shows should be improved in the library,
the tools, the examples and the paper itself. The goal is the pair: an
exceptional library and a paper that introduces it precisely. Items here
are worked off on `cpp` (library, tools, examples, docs) and on `cpp-paper`
(the paper); `cpp` is merged into `cpp-paper` as the code moves.

## 1. The paper

**Title:** *Paglets: Mobile Agents as Migrating WebAssembly Instances.* A
full paper, not an application note: it introduces the concept (a paglet is
a module plus one Wasm instance whose whole state is its linear memory; it
moves as a memory image between handlers), the event model, movement, the
mesh, the fault-tolerant zombie paglets, and the tools, with code excerpts
throughout and complete examples in the appendix. Companion to the Python
edition's application note (`overview-paper` branch).

**Where and how it is built.** `papers/paglets-cpp/paglets-cpp.tex` with one
file per chapter in `chapters/`; `make` runs latexmk. Code excerpts and
result tables are generated into the `.tex` files by `paglets-docs-sync`
from `--8<--` markers in the sources and from `docs/results/*.json`
(`% snippet:` and `% results:` markers; `make sync`, `make check`; the
`docs_sync` ctest fails while the paper is not current). Figures are
gnuplot from `figures/data.sh` (jq over the results) and TikZ in the text.
`tools/check-chapter.sh NAME` compiles one chapter alone.

**Publication.** The branch `cpp-paper` carries the paper and
`.github/workflows/cpp-paper.yml`: on push it builds the PDF and publishes
it to a GitHub release tagged `cpp-paper-YYYY-MM-DD` (not marked latest;
the Python note keeps `paper-*`), then starts the C++ docs workflow, which
puts the newest `cpp-paper-*` PDF at
`https://cklukas.github.io/paglets/cpp/paglets-cpp.pdf`; the docs overview
links to it. `cpp-docs.yml` must also carry the Python note's PDF
(`paper-*` release), which it did not until now.

### Structure

| Chapter | Content | Code and figures |
|---|---|---|
| 1 Introduction | why mobile agents again, why WebAssembly (a portable, sandboxed instance whose state is one linear memory), the contributions | — |
| 2 Background and related work | Aglets and its lifecycle; weak vs. strong mobility; process migration (CRIU) vs. migrating actors; virtual actors (Orleans); Wasm runtimes and migrating Wasm; the Python edition and what the C++ edition changes | — |
| 3 The paglet model | a module plus one instance: identity, owner, passport, trust class; messages, requests, replies; capabilities and endpoints as the only way to reach others; one handler at a time | a minimal paglet; the router with typed handlers; schema headers; `request` with a continuation; deferred replies |
| 4 Lifecycle and events | state diagram (created/cloned, active, inactive, dispatching, arrived, disposing, recovered); hooks as events delivered between handlers; failure reports instead of exceptions; `Started` with arguments and capabilities; clones | counter `on_created`; journey `on_dispatching`/`on_arrived`; `on_move_failed` fallback; `set_restart` and `on_recovered`; table of event documents and ordering guarantees |
| 5 Movement | what moves (linear memory and exported mutable globals as a memory image; no stacks, because moves happen between handlers); what does not (host resources: capabilities re-created or reported lost); transfer tickets; the move protocol between hosts as a sequence diagram (offer, module by hash, checks against passport/trust/residency, image transfer, location records, outcome); clones to other hosts; deactivation to disk; restart from the last image; code mobility and its limits (hashes, module trust, residency marks) | a touring paglet that keeps its state; fan-out of clones (inventory); the CLI's `dispatch` |
| 6 Communication across moves | endpoints that keep working while the target moves: location records, lookups, pins, delivery after a move; asynchronous sends and replies; delivery failures | two paglets talking while one moves |
| 7 The mesh | hosts, the signed ledger (admins, quorum, enrollment), passports; policy rules and grants; system paglets as the only path to resources; mesh-info and compute slots | a service contract and its typed client; a grant request |
| 8 Fault tolerance: zombie paglets | replicas on every host, roles, takeovers, journals, exactly one result at the sink; the protocol as a C++ state machine checked by `paglets-protocol-check`; checkpoints (ABI 1.2); what is proven for which world sizes and what is only tested (section 3 below) | a `patterns::Zombie<Work>` step |
| 9 Interfaces and tools | interface descriptions in the module; `paglets-web` and the CLI kept in step | brief |
| 10 Evaluation | image sizes and move times; move latency and transfer speed on the Mac + two Pi mesh; many instances on one host; the distributed pi computation; the Mesh Chronicle live run with failures and the protocol check results | from `docs/results/*.json` and `planning/cpp-m*-results.md` |
| 11 Discussion and limitations | the trust model; the 16 MB image limit and memory growth; interpreter speed; what weak mobility does not solve; Windows status; comparison table Aglets / Python edition / C++ edition | — |
| 12 Conclusion | — | — |
| A The paglet ABI | v1/v1.2: imports, exports, event documents, error codes | from `common/include/paglets/abi.hpp` |
| B Complete example paglets | counter (state, start arguments, interface); journey (a tour with events); inventory (fan-out); pi (compute slots); Mesh Chronicle (zombie), with commentary | whole-file snippets |
| C Lifecycle and event reference | every hook, when it runs, ordering diagram | — |
| D Reproducing the evaluation | build, `mesh_bench.sh`, `chronicle_live.sh`, `web_live.sh`, `update_results.sh` | — |

Conventions: plain prose, present tense, no hype; every technical claim
checked against the sources; excerpts come from the sources through the
markers, never by hand (hand-copied excerpts are listed in section 4 for
conversion); each chapter is verified against the code by a second pass
before it counts as written.

## 2. Verification of the zombie protocol: what the paper may claim

`paglets::verify` is an explicit-state model checker (every reachable
state of a finite world, breadth-first; safety on every state; recovery
properties as backward reachability of the goal over fault-free steps,
that is AG EF goal, the home-state property of Petri-net analysis). The
timed random runs are an addition for what the exhaustive check
abstracts away (real timer values, takeover times); they are not the
proof. The paper states this precisely, and the following tightening is
planned so its claims are sharp:

| Item | Status |
|---|---|
| V1. An exact mode without hash compaction for the two-replica worlds (states kept by encoding), so the two-replica results are exact and the compaction's omission probability (about n²/2¹²⁹) is only quoted for larger worlds | open |
| V2. A fair-cycle check: every bottom strongly connected component of the fault-free graph contains goal states (liveness under weak fairness), reported next to the recoverability result | open |
| V3. A mutation suite for the checker: known bugs seeded into the protocol (drop a hand-over, accept a stale beat, skip the sink number on redirect, …), each shown to be caught with a counterexample; recorded in the results | open |
| V4. The term abstraction (terms renamed, gaps shrunk to 2) written up as a soundness argument: the protocol compares terms only by order, equality and increment; an assertion in the model and a test that fails if a transition ever does more | open |
| V5. Three replicas: a full exploration (partial-order reduction or disk-backed state storage), or a written cutoff argument that n replicas behave like two or three; until then the paper says "to depth 11" | open |
| V6. The docs and planning say "symmetry reduction over replicas"; the code canonicalizes terms. Correct the docs (`planning/cpp-zombie.md`, `docs/zombie-protocol.md`) | open |
| V7. Replay of recorded live traces through the specification is the conformance check; state in the paper what the model leaves out (transport, gossip, the `zombies` service itself) | open |

## 3. Library, tools and examples: issues found while writing

Found by reading interfaces and examples with a reader's eyes. Each item
says what was found, why it matters, and what to do; "done" when it is in
`cpp`.

| Item | Found | Do | Status |
|---|---|---|---|
| L1 | | | |

## 4. Paper: gaps and polish

| Item | Found | Do | Status |
|---|---|---|---|
| P1 | | | |

## 5. Decisions

- **The paper is on its own branch** (`cpp-paper`), as the Python note is on
  `overview-paper`: `cpp` stays the code's branch, and the paper's workflow
  runs only there. Code changes the paper calls for go to `cpp`, then `cpp`
  is merged into `cpp-paper`.
- **Excerpts are generated.** `paglets-docs-sync` renders the same
  `--8<--` sections into LaTeX listings; a stale paper fails the
  `docs_sync` ctest, as a stale docs page does.
- **No ORCID, no venue formatting.** Published as a PDF on GitHub and
  linked from the C++ docs; a venue version is a later step.
