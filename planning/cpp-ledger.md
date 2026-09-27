# paglets/cpp: mesh ledger, keys and passports (WP8)

Status: implemented in `cpp/host/src/mesh` (library `paglets_mesh`), tested
in `cpp/tests/test_mesh.cpp`. Companion to
[cpp-security-and-communication.md](cpp-security-and-communication.md)
(sections 3.1, 3.2 and 3.5), which states the goals; this document records
the concrete design.

## 1. Keys

- Ed25519 through libsodium (1.0.18 or later): `SigningKey` keeps the secret
  in `sodium_malloc` memory (locked, guard pages, wiped on release).
- Key IDs are the lowercase hex form of the 32-byte public key.
- Key files are canonical MessagePack maps: `format` (`paglets-key-1`),
  `role` (`admin`, `owner`, `host`), `name`, `public`, `protection`, and for
  encrypted keys `salt`, `opslimit`, `memlimit`, `nonce` and `secret`.
- Admin and owner keys are always encrypted: Argon2id derives the key
  encryption key from a local passphrase (libsodium's "moderate" limits;
  tests use "interactive"), XChaCha20-Poly1305 encrypts the 32-byte seed,
  and the public key and role are associated data, so a key file cannot be
  relabeled. Files read from disk may not ask for more than the "sensitive"
  limits.
- Host keys may be stored unencrypted; a host must start without a person.
  Every key file is created with owner-only permissions (POSIX mode 0600,
  created exclusively; never overwritten).
- Loading refuses an unencrypted admin or owner key and checks that the
  public key matches the private one.

Open: platform key stores (Keychain, DPAPI/CNG) for host keys, the key
agent, hardware keys.

## 2. Canonical values

Everything that is signed is a canonical MessagePack value (`value.hpp`):
string map keys sorted by their bytes and not repeated, integers as 64-bit
signed values in their shortest form, shortest length headers, no floating
point and no extension types. The decoder rejects every other form, so a
decoded value re-encodes to the same bytes and no two encodings of one value
can carry different signatures.

## 3. Records

A record is `{body: bin, sigs: [[key, signature]]}`:

- `body` is a canonical map with `type`, `mesh` (the genesis ID; absent in
  genesis), `clock`, `epoch` (for admin records), `time` (informational) and
  `data`.
- The record ID is SHA-256 of the body bytes.
- Signatures cover `"paglets ledger record v1" 0x00 ID`, so signatures can be
  added without changing the ID. Replicas merge the signature sets of a
  record; this is how several admins co-sign a change that needs a quorum.
- Signatures are sorted by key, one per key; decoding verifies all of them.

Record types:

| Type | Signed by | Data |
|---|---|---|
| `genesis` | every initial admin | `name`, `admins` [key], `quorum` {`admin_set`, `default`} |
| `admin-set` | `quorum.admin_set` admins of its epoch | `add` [key], `remove` [{`key`, `keep` [record ID]}], `quorum`? |
| `host-enroll-request` | the host key itself | `key`, `name`, `labels` [str] |
| `host-enroll` | `quorum.default` admins | `key`, `name`, `labels` [str], `request`? |
| `host-remove` | `quorum.default` admins | `key` |
| `owner-enroll-request` | the owner key itself | `key`, `name` |
| `owner-enroll` | `quorum.default` admins | `key`, `name`, `groups` [str], `request`? |
| `owner-remove` | `quorum.default` admins | `key` |
| `request-deny` | `quorum.default` admins | `request`, `reason`? |
| `revoke` | `quorum.default` admins | `key`? or `record`?, `reason`? |

Records of other types are stored and replicated but have no effect, so
newer record types (policy rules and grants in WP9) pass through older hosts.

## 4. Deriving the state

A ledger is a grow-only set of records. Adding a record checks only its form,
its signatures and its mesh; its meaning is decided when the state is
derived, from the whole set, in a fixed order: by Lamport clock, then by ID.
Hosts with the same records derive the same state regardless of arrival
order. A new record's clock is one more than the largest clock its author's
ledger holds.

Clocks are chosen by the signer, so the order alone cannot tell whether an
admin signed a record before or after being removed. Two mechanisms close
that gap:

1. **Admin epochs.** Admin changes form a chain: each `admin-set` record names
   the epoch it changes (genesis or an earlier `admin-set`), and every admin
   record names the epoch its signers acted in. A signature counts only if the
   signer is an admin of that epoch.
2. **Keep lists.** Removing an admin lists the records that admin had signed
   so far (the admin CLI takes them from its ledger). A signature of a
   removed admin counts only on records in that list. Anything the removed
   admin signs later, with whatever epoch and clock, has no effect.

Several admin changes naming the same epoch conflict (concurrent changes, or
a removed admin forking an old epoch). Resolution:

- Every removal of every conflicting change applies ("removals win").
- Additions and quorum changes apply only from changes that still have their
  quorum without the signatures of admins the other changes remove. A
  removed admin's fork therefore adds nobody.
- The merged epoch's ID is SHA-256 over the sorted IDs of the conflicting
  changes; each of their IDs also names the merged epoch, so records made in
  one of them before the conflict was known stay valid.
- Quorums are capped at the number of remaining admins.

With `quorum.admin_set` 1, any single admin can change the admin set, and a
removed admin can still take part in a conflict at the epoch where it was
removed (its removals apply, as they could have before). Meshes that must
survive a stolen admin key use an admin-set quorum of 2 or more.

Then, in order:

- Revocations are collected first and win over everything regardless of
  order: a revoked key is removed from hosts and owners and can never be
  enrolled again; a revoked record has no effect. Genesis, admin changes and
  revocations cannot be revoked.
- Enrollments and removals apply in ledger order.
- Enrollment requests are pending until an enrollment names them, an admin
  denies them, or the key is enrolled or revoked. A request must be signed by
  the key it names (proof of possession).

The state lists every record without effect together with the reason
(`LedgerState::ignored`), for administrators and audits. Its canonical form
and digest (`LedgerState::digest`) compare the state of two hosts.

Storage: one file per record (`<id>.rec`, written atomically) in the
ledger directory. A host opens its ledger with the mesh ID as its trust
anchor.

Open: checkpoints and pruning, key rotation records (today: enroll the new
key, revoke the old one; admins: one admin change that adds and removes).

## 5. Gossip

`Replica` replicates a ledger over a `GossipTransport` (frames addressed by
host key; in-memory in tests, the host-to-host channels of WP12 later):

- **Push**: a record or signature new to a replica is forwarded to every
  peer except its sender; a replica that already has it forwards nothing.
- **Anti-entropy**: `tick()` sends the ledger digest (hash over record IDs
  and signature sets) to the next peer. A peer with another digest answers
  with its inventory (record IDs and signature-set hashes); the first
  replica pushes what the peer lacks and asks for what it lacks.
- Peers are the enrolled hosts plus configured seeds.
- Frames are canonical values; records are verified on arrival, so a peer
  can only relay validly signed records. Limits: 16 MB per frame, 512
  records per push.

Open: inventories are complete lists (fine for thousands of records; range
digests later), no rate limits per peer yet (WP21).

## 6. Passports

- **Root**: canonical body `{type: "passport", mesh, owner, module, paglet,
  manifest, issued, expires}`, signed by the owner over
  `"paglets passport v1" 0x00 body`.
- **Links** for children and clones: `{type: "passport-link", kind, parent,
  paglet, module, host, prev, issued}`, signed by the creating host over
  `"paglets passport link v1" 0x00 body`. `prev` is SHA-256 of the previous
  element's body, so a link cannot be moved to another chain; `parent` must
  be the previous element's paglet.
- **Verification** (`verify_passport`): signatures and chain (on decoding),
  then against the ledger state: same mesh, owner enrolled and not revoked,
  not expired, every linking host enrolled and not revoked, and the last
  element names the expected paglet and module.

A host in a mesh creates root paglets from their passports
(`node::Node::create`): the passport must verify for the module, and the
paglet gets its ID and owner; the host keeps the passport with the paglet.
Manifests are evaluated by the policy (planning/cpp-policy.md, section 6).

Open: host-signed links for children and clones as the runtime creates
them, and checks on arrival (M3, WP11/WP12).
