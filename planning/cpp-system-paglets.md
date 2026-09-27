# paglets/cpp: system paglets (WP10)

Status: framework implemented in the runtime (`cpp/host`, `runtime/system.hpp`),
tested in `cpp/tests/test_system.cpp`. Companion to
[cpp-security-and-communication.md](cpp-security-and-communication.md)
(sections 4 and 6), which states the goals, and
[cpp-abi-v1.md](cpp-abi-v1.md) (section 13, the guest side).

## 1. Native system paglets

A native system paglet is a C++ object in the host process that is a paglet
for everybody else (`runtime::SystemPaglet`):

- It is registered with `Runtime::add_system_paglet` and gets the paglet ID
  `system.<name>` (for example `system.files`), a mailbox, and serial
  handling of its messages on the scheduler lanes, like any paglet.
- It has no Wasm instance and no memory image; it never deactivates, moves
  or ends, and `dispose`/`deactivate` on it are refused.
- `handle(ctx, call)` runs for one message at a time. `call` gives the name,
  payload, host-stamped sender record, badge, lent and transferred
  capabilities, and answers requests (`reply`, or `defer` and answer later).
  A request left unanswered is answered `internal`; transferred capabilities
  the service does not take are released.
- `paglet_ended(ctx, id)` tells it, in order with its messages, that a
  paglet ended, so it can release what it kept for that paglet.
- The `SystemContext` mints resource capabilities provided by the service,
  checks lent ones (`check`: revoked, expired, wrong kind or provider,
  missing right), makes endpoints and sends messages to paglets.
- Native handlers are trusted host code: no time budget, no sandbox. Long
  blocking work belongs on a separate pool (open; today file operations run
  on the lane).

## 2. Service endpoints

`default_ops()` names the operations of the endpoint every new paglet
receives (security design, section 7.5). The runtime installs these
non-transferable endpoints when a paglet is created (host, child), in
service name order, and lists them in `self_info.services`. Clones keep the
handles of their original, so handles stored in memory stay meaningful.
System paglets are registered when the host starts, before paglets are
created or recovered.

## 3. Resource capabilities

`Cap::Kind::resource` capabilities carry a type (`dir`, `file`, `artifact`,
`topic`), a resource string defined by the providing service (for `dir`: a
named root and a relative path, `root/sub/dir`), rights in `ops`, and
optionally a grant, an expiry and a use limit. The provider is the target.

- Guests use them by **lending**: the handle goes into `lend` of a request
  to the providing service, which sees a copy for that message only. This is
  the confused-deputy protection of the security design (section 6.5): a
  service acts on what the caller shows it, never on its own authority.
- **Derive** narrows rights and, for `dir`, the path; paths are relative,
  `/`-separated, without `.`, `..`, empty segments, `\` or `:`, so a derived
  capability can never point outside its parent.
- **Transfer** moves them between paglets like endpoints.

## 4. Revocation tree

Every endpoint and resource capability has an ID (128 random bits).
Derived capabilities record the IDs they descend from (`lineage`) and keep
the grant of their root. The runtime refuses a capability with `revoked`
when its ID, an ancestor's ID, or its grant is revoked:

- `Runtime::revoke_capability(id)` revokes a subtree on this host; the IDs
  are stored in the state directory.
- `Runtime::set_revoked_grants(...)` receives the grants revoked in the
  ledger (WP9); every copy and derivative of a granted capability fails on
  every host as soon as the revocation record arrives there.

Transferred copies keep their ID, so revocation reaches them in the
receiver's table too. Revoked capabilities stay in the table (inspect shows
`revoked`) until dropped.

## 5. Service contracts

A service is described once, as plain C++ declarations in a schema header
that both the host (GCC with reflection) and guests (clang, through the
schema generator) compile (`wire/schema.hpp`):

- **Wire types**: enums and structs of the namespace, encoded as
  MessagePack maps keyed by field name (as before).
- **Contract**: a struct with `static constexpr std::string_view service`
  and one member function declaration per operation, taking the request
  type and returning the reply type. The operation name is the function
  name and the message name.

From the same declarations:

| Side | Mechanism | What it gives |
|---|---|---|
| Host | `services::ContractPaglet<Contract, Impl>` (reflection) | a native system paglet: decodes the request, calls `Impl::<op>(request, Operation&)`, encodes the reply or answers the returned error; `describe` |
| Guest | schema generator | codecs; `Client` with one typed method per operation (`on_reply(Result<Reply>, Message&)`); `serve(router, impl)` registering an implementation; `describe` |
| Both | `wire::namespace_descriptor` | the JSON descriptor `{namespace, types, services}`, byte-identical on host and guest |

Application services use the same mechanism: a guest that serves a
contract calls the generated `serve`, and callers use the generated
`Client`, whether the service is a guest or a native system paglet.
System paglets and their contracts need C++26 reflection on the host; the
host-only build without reflection leaves them out.
