# paglets/cpp: policy and grants (WP9)

Status: implemented (WP9) in `cpp/host/src/mesh/policy.cpp` (rules,
evaluation, manifests), `cpp/host/src/mesh/ledger.cpp` (records) and
`cpp/host/src/node/` (the `grants` system paglet and the glue between
ledger and runtime); tested in `cpp/tests/test_policy.cpp`. Companion to
[cpp-security-and-communication.md](cpp-security-and-communication.md)
(section 7), [cpp-ledger.md](cpp-ledger.md) and
[cpp-system-paglets.md](cpp-system-paglets.md).

## 1. Records

New ledger record types (the ledger's derivation, section 4 of
cpp-ledger.md, gives them meaning):

| Type | Signed by | Data |
|---|---|---|
| `policy-rule` | admin quorum | `name`, `decision` (allow, ask, deny), `service`, `ops`, `match`, `scope`?, `max_duration_ms`?, `priority`? |
| `grant-request` | the host of the paglet, or the owner (early approval) | `paglet`, `owner`, `module`, `trust`, `host`, `items` [item], `duration_ms`, `reason` |
| `grant` | admin quorum (approval), or a host (derived from an allow rule) | `paglet`, `owner`, `item`, `hosts`, `expires`, `request`? (approval), `rule`? (derived), `module`, `trust` |
| `grant-release` | a host | `grant` |
| `audit` | a host | `paglet`, `owner`, `item`, `decision`, `rule`?, `grant`?, `request`? |

An **item** is one piece of access: `{service, ops, root?, path?}`. For
`files`, `ops` are rights (read, write, create, delete) and `root`/`path`
name a directory; for other services `ops` are operations.

`match` selects principals: `owners` [key], `groups` [str], `modules`
[hex hash], `signers` [key] (modules with a valid signature of one of them,
[cpp-modules.md](cpp-modules.md), section 4), `trust` [str], `hosts`
{`keys` [key], `labels` [str]}. Absent
members match everything. `scope` limits files items: `roots` [str] and
`paths` [pattern] (`*`, `?`, `**` as in `files.find`, matched against the
item's path; an item for a directory is covered when the pattern matches the
directory or an ancestor ends in `/**`).

## 2. Evaluation

`evaluate(state, principal, item)` on the paglet's host:

1. Rules that match the principal and the host, name the item's service
   (or `*`), cover all its operations (or list `*`) and cover its scope.
2. The most specific rule wins: highest `priority`, then most `match`
   members, then a `scope` over none, then `deny` over `ask` over `allow`,
   then the rule order of the ledger. No matching rule: `deny`.

Every host evaluates the same rules from its ledger copy, so decisions do
not depend on which host a paglet is on, only on the host selector.

## 3. Grants

- **allow**: the host signs a `grant` naming the rule; it expires at most
  `max_duration_ms` after issue. Other hosts accept such a grant only if it
  is signed by an enrolled host, the rule is valid and allows it, and the
  principal in the grant matches the rule; a host cannot mint access its
  rules do not give.
- **ask**: the host signs a `grant-request`; it is pending until an admin
  signs a `grant` naming it (approval, from any host) or a `request-deny`.
- **deny**: no record but the audit entry.

A grant is valid until it expires, is revoked by an admin (`revoke`), or
released by a host (`grant-release`). Revocations and releases win
regardless of order.

## 4. The `grants` system paglet

Default endpoint of every paglet: `request(item, duration, reason)`,
`status(request)`, `release(grant)`, `list()`.

- `request` evaluates and answers `granted` (with the capability),
  `pending` (with the request ID) or `denied` (with the rule, if any).
- When an approval arrives by gossip, the host of the paglet materializes
  the capability and sends it to the paglet as message `grants.granted`
  (denials as `grants.denied`).
- **Materialization**: a files item becomes a `dir` capability for the
  root and path with the rights, the grant's ID and expiry; an item of a
  service with ambient authority (server-info) becomes an endpoint with the
  operations. Capabilities carry the grant, so the runtime refuses them
  once the grant is revoked or released on any host (revocation tree).
- Grants of a paglet materialize on every host the grant's host selector
  covers, so they follow the paglet (M3).

## 5. Services with ambient authority

`files`, `artifacts` and `pubsub` act only on capabilities the caller
lends, and `directory`, `storage`, `user-info` and `grants` only on the
caller's own things; their endpoints need no policy. `server-info` reads
host state without a capability: its endpoint from the directory carries
only the operations the rules allow (`SystemPaglet::ambient()`).

## 6. Manifests

A passport's manifest (section 3.5 of the security design) is a list of
items. `preflight(state, principal, manifest, host)` evaluates each item
for a target host, so a transfer can require the manifest to be satisfied.
For early approval an owner signs a `grant-request` with the manifest
items for the paglet ID it will create; an admin approves it once, and the
grants materialize when the paglet starts on a covered host.

## 7. Audit

Every decision leaves a record: hosts sign `audit` records for allow, ask
and deny; approvals, denials of requests and revocations are admin records
themselves. They replicate with the ledger, so admins query them from any
host (`LedgerState::audit`, `paglets-host ledger audit`).

## 8. Administration

`paglets-host ledger rule` adds rules (`--decision`, `--service`, `--op`,
match options `--owner`, `--group`, `--module`, `--signer`, `--trust`, `--host-label`,
scope options `--root`, `--path`, `--max-duration`, `--priority`);
`ledger approve` and `ledger deny` decide grant requests as well as
enrollment requests; `ledger revoke` ends grants; `ledger show` lists rules,
grants and pending grant requests; `ledger audit` lists the audit log.

## 9. WP9 exit

`cpp/tests/test_policy.cpp` runs two hosts connected by gossip. Host A runs
the explorer paglet with the system paglets and the node; the admin works
on host B. Without a rule the request is denied; after an `ask` rule it is
pending, the request reaches host B, the admin approves it there, and the
approval reaches host A, which hands the paglet its `dir` capability; the
paglet reads the file. The admin revokes the grant on host B and the
capability fails with `revoked` on host A. An `allow` rule grants at once
for at most its duration; the capability fails with `expired` afterwards,
and the host-derived grant is valid on host B as well. Both hosts hold the
same audit log: deny, ask, allow.

## 10. Open

- Grants follow paglets to other hosts with M3 (materialization on arrival
  is the same code path as approvals).
- Rate limits for requests and audit records per paglet (WP21).
- The host's named roots come from its configuration; moving them into the
  host's enrollment record (security design, section 3.2) comes with WP12.
