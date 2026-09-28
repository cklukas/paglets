# paglets/cpp: location and pinning (WP13)

Status: implemented. Companion to the plan (WP13) and to
[cpp-security-and-communication.md](cpp-security-and-communication.md)
(sections 5.2 and 5.3), which state the goals; this document records the
concrete design. Code: `cpp/host/src/node/location.cpp` (the node's side),
`cpp/common/include/paglets/services/locator.hpp` (the contract), pins in
the runtime (`Runtime::pin`); tests in `cpp/tests/test_location.cpp`.

## 1. Location records

Every paglet has a **location record**: the host that holds it, its move
counter, when it arrived there (or was created) and its owner. No host is
special; records are spread over the hosts:

- **Responsible hosts.** Consistent hashing over the hosts that are up: each
  enrolled host has 16 points on a 64-bit ring (SHA-256 of
  `"paglets ring v1" 0x00`, the host key and the point number), a paglet has
  one (the same hash over its ID). The first 3 distinct hosts clockwise from
  the paglet's point keep its record; with 3 or fewer hosts, all do.
- **Move counters.** Creation writes counter 0 (root paglets, children and
  local clones, from the host that made them). Every move increments it;
  the counter travels in the signed move offer. A record with a higher
  counter replaces one with a lower counter; the same counter from the same
  host only refreshes it. A stale record never overwrites a newer one.
- **Majority update on every move.** When the destination is prepared
  (`move-ready`, planning/cpp-networking.md, section 6), the source sends the
  new record to the responsible hosts and commits the move only after a
  majority of them acknowledged it. If a responsible host already has a
  higher counter (counters lost in a crash), the source records above it (at
  most 3 times). Without a majority within the move timeout the move is
  aborted, the records are overwritten with the source (and a higher
  counter), and the paglet stays with `paglets.move_failed`. The destination
  takes the final counter from `move-commit`.
- **Refresh and expiry.** Holders re-send the records of their paglets every
  5 minutes; records not refreshed for an hour end (paglets that ended).
  Records are kept in the node's state directory (`locations`).

## 2. Liveness and rebalancing

- Every host sends a heartbeat frame (`hb`) to every enrolled host every 2 s
  and notes when it last heard from each (any frame counts). A host not
  heard from for 10 s is down in this host's view; a host that just appeared
  in the ledger is up until it stays silent. A host that did not run for
  longer than that (suspended) forgets what it last heard instead of
  declaring everybody down.
- The ring is built over the view. When the view changes (a host joins,
  leaves the ledger, fails or returns), records reach the hosts that are
  responsible now: holders send the records of their paglets, and every host
  sends the records it keeps to hosts that became responsible for them.
- Transfer tickets try hosts that are up first (`any` and labels also
  shuffle their hosts, so paglets spread).

Different hosts can briefly disagree about the view; majorities on both
sides still overlap in all but the most unlucky partitions, and a lookup
that finds no record asks the whole mesh (section 3).

## 3. Lookups

A lookup (`Node::locate`, the `locator` system paglet, CLI sessions, message
forwarding) runs:

1. **Here.** A paglet on this host is found at once.
2. **Records.** The responsible hosts are asked (`loc-get` / `loc-rec`);
   with answers from a majority, the record with the highest counter wins.
   Since every move commits with a majority, the answer is the latest
   committed location. Each step waits at most 3 s.
3. **Who holds P.** If no responsible host knows the paglet (records lost,
   a partition), every live host is asked (`loc-who`); the host that holds
   it answers (`loc-here`).

**Location caches**: hosts remember the answers of recent lookups and moves
for 10 s; message forwarding uses them before asking. **Tombstones**
(planning/cpp-networking.md, section 4) still forward messages for 10
minutes after a move; when a host has neither the paglet nor a tombstone,
the runtime hands messages and requests to the node (`MobilityHooks::
unresolved`), which looks the paglet up and forwards them (at most 4 times
in all), or answers requests with `not_found`.

## 4. Pins

A pin keeps a paglet on its host for a while:

- **Runtime.** `Runtime::pin(paglet, pin, until)`: while a pin has not
  ended, the paglet's own `dispatch` returns `pinned` (ABI v1.1, error
  -17) and `self_info.pinned_until` names the end of its last pin;
  `Runtime::dispatch` refuses with `pinned`; a dispatch requested earlier
  fails with `paglets.move_failed` when it comes due. Pinning a paglet that
  is leaving (or about to) answers `bad_state`, so a pin never races a move.
  Cloning, deactivation and messages are unaffected. Several pins can be
  held; the paglet can move once all have ended.
- **Leases.** Pins end at their expiry, when the holder releases them, or
  when an admin ends them. The node keeps them in its state (`node.state`)
  and applies them again when the host restarts, so a pinned paglet stays
  pinned across restarts.
- **Following the paglet.** A pin goes to the host a lookup found (frame
  `pin`). That host answers `pin-ok` (with the end), `pin-moved` (with its
  tombstone's host, or without one: the lookup starts again), `pin-busy`
  (the paglet is in the middle of a move: the lookup waits 100 ms and starts
  again) or `pin-no`. A lookup follows at most 8 redirects and 40 waits.

## 5. The `locator` system paglet

Every paglet gets an endpoint to it (default operations `locate`,
`locate_and_pin`, `release`; contract in
`cpp/common/include/paglets/services/locator.hpp`):

| Operation | Lent capability | Reply |
|---|---|---|
| `locate` | an endpoint to the paglet with the `locate` right (or `*`) | `Location`: paglet, host key ID and name, when it moved there, move counter |
| `locate_and_pin` | an endpoint with the `pin` right (or `*`) | `Pin`: the location, the pin's ID and end; the `pin` capability as the first capability |
| `release` | the `pin` capability | `released` (false if the pin had ended) |

- **Policy.** `locate_and_pin` needs a rule that allows service `locator`,
  operation `pin`, for the caller (planning/cpp-policy.md); the rule's
  `max_duration_ms` caps the pin (1 hour without one). Without such a rule,
  pinning is denied.
- **Pin capabilities** are resource capabilities of kind `pin` (right
  `release`) naming the paglet, the pin and the host that holds it. They
  stay valid when their holder moves (every host has a locator); releasing
  revokes the capability and its copies.
- The answers are deferred replies: the locator answers when the lookup
  ends.

## 6. Frames

All on the host channels, from enrolled hosts only:

| Frame | Members | Meaning |
|---|---|---|
| `hb` | | heartbeat |
| `loc-set` | `p`, `h`, `c`, `tm`, `o`, `r`? | a record (acknowledged with `loc-ack` when `r` is given) |
| `loc-ack` | `r`, `p`, `h`, `c`, `tm`, `o` | the record the host keeps now |
| `loc-get`, `loc-rec` | `r`, `p` (+ the record, if known) | a responsible host is asked |
| `loc-who`, `loc-here` | `r`, `p` (+ the record) | the mesh is asked; holders answer |
| `pin` | `r`, `p`, `pin`, `until`, `holder`, `reason` | pin the paglet here |
| `pin-ok`, `pin-moved`, `pin-busy`, `pin-no` | `r`, ... | answers (above) |
| `pin-release`, `pin-released` | `r`, `p`, `pin` / `ok` | a pin's holder releases it |
| `pin-force`, `pin-forced` | `r`, `p`, `pin`? / `n` | an admin ends pins (all, or one) |

## 7. Admins and CLI sessions

CLI sessions (planning/cpp-networking.md, section 8) add:

| Request (`t`) | Members | Who | Answer |
|---|---|---|---|
| `locate` | `paglet` | admins, the paglet's owner | host, host name, moves, time of the move |
| `pin` | `paglet`, `duration_ms`, `reason` | admins, the paglet's owner | the location, the pin and its end (at most 24 hours) |
| `pins` | | admins (all), owners (their paglets') | pins on this host |
| `unpin` | `paglet`, `pin`? | admins | ends the paglet's pins wherever it is |

On the command line: `paglets-host remote locate|pin|pins|unpin`.

## 8. Exit

The exit of WP13: a paglet moving continuously between hosts can be
located and pinned, also after any one host is shut down; while it is
pinned, its dispatch returns `pinned` until the pin ends.

| Test | Covers |
|---|---|
| `test_location.cpp`: records | responsible hosts agree; a move commits only after a majority recorded it; every host finds the paglet; clones on other hosts |
| who holds | records lost: the mesh is asked |
| failover | a responsible host fails: the others notice, its share moves on, moves still commit; when it returns, its stale record loses |
| locator | locate and pin from a paglet, the policy's maximum, `pinned` from dispatch and `pinned_until`, release, revoked pin capabilities, rights on the endpoint |
| pins | no rule, no pin; several pins; pins survive a restart; admins end them |
| forwarding | a host without the paglet or a tombstone looks it up and forwards a request |
| exit | a child paglet dispatches itself to `any` host every 20 ms; it is located and pinned (by its creator through the locator, and by hosts) and stays while pinned (`roam:pinned`); then a host that keeps its record goes down, and it is still located and pinned from every host |
| `host_serve_move` | over HTTPS with `paglets-host serve`: `remote locate`, `remote pin` (a dispatch is refused), `remote pins`, `remote unpin` by an admin through the other host |
