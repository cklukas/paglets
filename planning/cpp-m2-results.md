# paglets/cpp: milestone M2 progress

Status: in progress. WP8 and WP10 are done; WP9 is next. Companion to
[cpp-edition-plan.md](cpp-edition-plan.md); M1 is closed in
[cpp-m1-results.md](cpp-m1-results.md).

## Summary

| Work package | Exit criterion | State |
|---|---|---|
| WP8 Mesh ledger and identity | Three hosts converge on the same state after partitions; an admin approves an enrollment from any host; records from a removed admin are ignored | **Met**: test "gossip: three hosts converge after partitions (WP8 exit)" (two partitions; an owner request made on one host is approved on another; a removed admin's later record is ignored everywhere). Design in [cpp-ledger.md](cpp-ledger.md) |
| WP9 Policy and grants | A roaming paglet gets file access only after a rule or an admin approval; every decision is audited | Open |
| WP10 System paglet framework | The same roaming paglet reads files and reports load and free space on macOS, Linux and Windows with identical schemas | **Met**: test "services: a roaming paglet finds and reads files and reports load and space (WP10 exit)" on all CI platforms. Design in [cpp-system-paglets.md](cpp-system-paglets.md) |

## WP8: what was built

- **libsodium** (1.0.18 or later) is the cryptography library of the host:
  `cmake/FindSodium.cmake`, packages in every CI job. Worker processes do not
  link it.
- **Keys** (`mesh/crypto.hpp`): Ed25519 signing keys in protected memory;
  key files for admins, owners and hosts; admin and owner keys encrypted
  with Argon2id and XChaCha20-Poly1305 (public key and role bound as
  associated data), created with owner-only permissions, never overwritten.
- **Canonical values** (`mesh/value.hpp`): one MessagePack encoding per
  value; everything else is rejected when decoding.
- **Records** (`mesh/record.hpp`): canonical bodies, SHA-256 IDs, signature
  sets that merge (co-signing for quorums).
- **Ledger** (`mesh/ledger.hpp`): one file per record, opened against the
  mesh ID as trust anchor; state derived deterministically from the record
  set: admin epochs with quorums, keep lists against backdated records of
  removed admins, "removals win" for conflicting admin changes, revocations
  that win regardless of order, host and owner enrollment by request and
  approval, denials, removals, and the list of records without effect with
  reasons.
- **Gossip** (`mesh/gossip.hpp`): eager push and digest/inventory
  anti-entropy over a transport interface; tests use an in-memory network
  with partitions.
- **Passports** (`mesh/passport.hpp`): owner-signed roots, host-signed links
  for children and clones chained by hash, verification against the ledger
  state.
- **Command line**: `paglets-host keys init|show`, `mesh create` and
  `ledger show|request|approve|deny|enroll|remove|revoke|admins|sign` on a
  local ledger directory (CTest `host_cli_ledger`).
- **Tests**: 16 new unit tests (values, keys, records, ledger rules, gossip,
  passports), clean under ASan + UBSan and ThreadSanitizer.

## WP10: what was built

- **Native system paglets** in the runtime (`runtime/system.hpp`): C++
  objects with a paglet ID, mailbox and serial handling; lent and
  transferred capabilities, deferred replies, notifications when paglets
  end.
- **ABI v1.1** (compatible): resource capabilities (`dir`, `file`,
  `artifact`, `topic`), lending, derive paths, service endpoints in every
  paglet (`self_info.services`), `revoked`.
- **Revocation tree** (from WP6): capability IDs and lineage; revoking an
  ID or a grant invalidates every copy and descendant.
- **Service contracts** from reflected declarations: host dispatch
  (`ContractPaglet`), generated guest clients and `serve` for guest
  services, identical descriptors.
- **Platform layer** for Linux, macOS and Windows, and the seven standard
  system paglets: files (named roots, rights, path safety), server-info,
  directory (with a policy hook for WP9), storage, artifacts, pubsub,
  user-info.
- **Tests**: conformance C31–C35, contracts, and the services driven by one
  guest through the generated clients (including symbolic link escapes on
  POSIX), clean under ASan + UBSan and ThreadSanitizer.

## Findings

1. **Signer-chosen clocks cannot order an admin's removal against the
   admin's own later records.** A removed admin can always pick a small
   clock and an old epoch. The removal therefore lists the records of that
   admin it keeps; everything else signed by the admin has no effect. The
   same applies to admin changes forking an old epoch.
2. **A conflict must not orphan records made in good faith.** Merging
   conflicting admin changes into a new epoch ID would invalidate records
   that named one of the changes; the changes' IDs therefore remain names of
   the merged epoch.
3. **With an admin-set quorum of 1 a removed admin can still remove others**
   through a conflict at the epoch of its own removal (it could have done so
   before, too). Meshes that must survive a stolen admin key use an
   admin-set quorum of 2 or more; `mesh create --admin-quorum`.

4. **Services need lending, not transfer.** Passing a directory capability
   to `files` by transfer would take it from the caller at every call;
   lending (ABI v1.1) shows it for one message and keeps the confused-deputy
   protection: the service acts only on what the caller shows.
5. **Case-insensitive file systems** (macOS, Windows) would merge storage
   keys that differ in case; keys are stored as hex file names.
6. **`linux` is a predefined macro** in GNU language modes, so the OS is a
   string in the server-info schema, not an enum.

## Next steps

1. WP9: policy rules, grants system paglet, capability manifests, audit;
   host glue between the ledger and the runtime (revoked grants, directory
   policy, materialized `dir` capabilities).
2. Passport checks in the runtime once a host runs with a ledger (WP11,
   WP12); ledger checkpoints and pruning; platform key stores for host keys
   and a key agent.
