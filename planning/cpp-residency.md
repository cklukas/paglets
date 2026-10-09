# paglets/cpp: data residency (WP17)

Status: implemented for paglets and artifacts. Companion to the plan (WP17)
and to the security design, section 7.4. Code: residency rules in
`cpp/host/src/mesh/ledger.cpp` (record `root-residency`), marks in
`cpp/host/src/runtime.cpp`, marking in `cpp/host/src/services/files.cpp`
and `artifacts.cpp`, enforcement in `cpp/host/src/node/movement.cpp`; tests in `cpp/tests/test_movement.cpp`,
`test_services.cpp` (artifacts) and `test_gateway.cpp` (the digest).

## 1. Rules

Admins give a named root a residency rule (record `root-residency`, signed
by the admin quorum like every admin record):

| Rule | Meaning |
|---|---|
| `host-only` | content read from the root stays on the host it was read on |
| `hosts` | content goes only to the hosts a selector chooses (host keys, labels) |
| `none` | lifts the root's rule |

The latest rule for a root holds. On the command line:

```text
paglets-host ledger residency --ledger DIR --admin KEY... clinic host-only
paglets-host ledger residency --ledger DIR --admin KEY... clinic hosts --host-label site
paglets-host ledger residency --ledger DIR --admin KEY... clinic hosts a b
paglets-host ledger residency --ledger DIR --admin KEY... clinic none
```

`paglets-host ledger show` lists the rules under "data residency".

## 2. Marks

A paglet carries what it read in its memory, so residency applies to the
paglet as a whole. When a paglet reads from a named root through `files`
(listing, finding or reading, with a capability that has the `read`
right), its host marks the paglet with `root:<name>`, whether or not the
root has a rule yet. Artifacts keep the marks of the paglets that stored
them (also downloads by `web`), and a paglet that reads an artifact takes
its marks. Marks:

- are never removed (the host cannot know what the paglet kept);
- travel with the paglet and are stored with its checkpoints;
- pass to its children and clones.

## 3. Enforcement

At departure (dispatch, and clones sent to another host) the source host
checks every mark against the rules in force:

- a mark of a `host-only` root: the move fails ("the content of root R
  stays on this host"); the paglet stays and gets `move_failed`;
- a mark of a `hosts` root: only hosts the selector chooses are
  candidates. Hosts it leaves out appear in the failure reason when no
  host remains.

A rule made after the paglet read the root applies too, because the mark
names the root, not the rule.

## 4. Not yet

- A `declassify` step that admins allow for a specific module.
- Mounts (files as WASI directories) do not exist yet; they will mark like
  `files` reads.
