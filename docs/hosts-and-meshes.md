# Hosts and meshes

A mesh is a set of hosts that trust the same signed **ledger**. The ledger
records:

- who belongs to the mesh: hosts, admins, owners and signers, each with a
  key;
- the policy rules for host resources;
- which modules are trusted;
- where data may go.

Hosts spread ledger records among themselves by gossip. All examples on
this page use `paglets-host` from a build directory:

```bash
H=build/linux-gcc16/host/paglets-host
```

## Keys and the ledger

Admin, owner and signer keys are encrypted with a passphrase. The
passphrase is read from the terminal, or from `--passphrase-file`. Host
keys are stored unencrypted and readable only by their owner, because
`serve` opens its key without a passphrase. (`keys init` encrypts a host key
too when you give `--passphrase-file`, but `serve` cannot use such a key.)
`keys init` prints the new key's ID. The ledger commands work on a local ledger directory, either
a host's copy or an admin's.

```bash
$H keys init --role admin --name alice --out alice.key
$H keys init --role host --name lab-1 --out lab-1.key
$H mesh create --name lab --ledger ledger --admin alice.key
$H ledger request --ledger ledger --key lab-1.key --label linux   # prints the request ID
$H ledger show --ledger ledger                                     # lists pending requests
$H ledger approve --ledger ledger --admin alice.key <request-id>
$H ledger revoke --ledger ledger --admin alice.key <key-id> --reason retired
```

Host labels (`--label`) and owner groups (`--group`) are the attributes
selected by policy and residency rules. For direct enrollment, add labels
to `ledger enroll ... host ...` and groups to `ledger enroll ... owner ...`.

Once a host learns a revocation or admin removal, it checks the current
membership on existing channels as well as new handshakes. This includes
queued frames, relay tunnels and long-poll results. A bootstrap seed does
not override an explicit key revocation. Work already admitted can finish,
but channel replies are checked again before release. Clients must learn
ledger changes through their local ledger copy; revocation is not global
until that change has reached the relevant host.

More ledger commands:

- `enroll` and `remove` add and remove members directly;
- `deny` rejects a request;
- `admins` adds or removes admins and changes the quorum;
- `sign` co-signs a record that needs several admins.

### Quorums

The ledger has two signature thresholds. `--admin-quorum` controls changes
of the admin set and the thresholds themselves; `--quorum` controls other
admin records, such as enrollment and policy changes. `mesh create`
defaults to an admin-set quorum of 2 with three or more initial admins,
otherwise 1; the other quorum defaults to 1. Explicit options take precedence.
With several admins, a quorum of 1 lets any one admin change the admin set,
so creation prints a note. Use `--admin-quorum 2` or more to require agreement.

Existing ledgers retain their recorded thresholds. Use `ledger admins`
with enough current admins' signatures to change them. A record missing
signatures has no effect until another admin adds them with `ledger sign`.
Concurrent admin removals win; additions and quorum changes count only if
their signatures still meet the quorum after those removals. See the
ledger design for the full conflict rules.

The [command-line reference](cli-reference.md#ledger) lists every ledger
command and option. The
[ledger design](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-ledger.md)
describes the records and their signatures.

## Paglet identities and upgrades

New root passports use version 2. Their 32-digit paglet ID is the first
16 bytes of SHA-256 over a canonical record containing the domain
`paglets root identity v2`, mesh ID, owner public key and owner-local
launch nonce. Hosts recompute this ID before accepting the passport. A
valid owner signature cannot claim another owner's chosen global ID.
Children and clones retain their host-signed passport links; moves retain
the same ID.

Launch normally generates a random nonce. `remote launch --nonce NONCE`
lets an owner prepare a repeatable identity, for example for grant approval
before launch; the nonce has 1–128 bytes. Choose a fresh nonce for each new
paglet. Reusing it selects the same identity, even when the module changes;
it is not a mesh-wide reservation or a new incarnation. The previous
`--id` option is refused with a migration message. The C++ host client
`Session::launch` and `Passport::issue` now take a nonce in that argument
position; read the resulting ID from the returned value or `paglet()`.

This change uses **mesh protocol 2**. Upgrade hosts, CLI tools and the web
UI together; protocol 1 channels are refused. Existing state directories,
ledgers, saved passports and paglet IDs are retained. Saved version 1
paglets can recover and move between upgraded hosts, whose authenticated
host signatures vouch for the transfer. Version 1 passports cannot launch
new roots through local, owner-session or forwarded mesh launch requests.
Holder metadata is saved on creation and arrival. Upgrades also recover
independently saved passports when an older host left no node metadata file.
This preserves the enrolled-host trust model: a compromised enrolled host
can still lie about the paglets it runs.

## Policy

Rules decide whether a request for a host resource is **allowed**, **asked**
or **denied**. Paglets ask the `grants` system paglet. Admins approve or
deny the asked requests from any host. Every decision lands in the audit
log. An approval belongs to the complete paglet principal: ID, owner,
module hash and trust class. Reusing an ID with a different principal does
not transfer its approvals or grant notifications. The same identity check
applies to compute-job admission and grants recreated after movement.

```bash
$H ledger rule --ledger ledger --admin alice.key --name "staff docs" --decision ask \
    --service files --op read --group staff --root data --path "docs/**"
$H ledger show --ledger ledger                     # rules, grants, pending grant requests
$H ledger approve --ledger ledger --admin alice.key <grant-request-id>
$H ledger audit --ledger ledger
```

Among matching rules, the winner is chosen in this order:

1. Higher `--priority` (default 0).
2. More match fields: owner, group, module, signer, trust class
   and host each count as one field when present.
3. A resource scope, such as `--root`, rather than no scope.
4. `deny` before `ask` before `allow`.
5. Earlier ledger order.

For example, at equal priority the staff rule below wins for staff reading
`data`, because it has a group match and a resource scope. The broad denial
does not automatically override it. Give the denial `--priority 1` to make
it win instead:

```bash
$H ledger rule --ledger ledger --admin alice.key --name "deny file reads" --decision deny \
    --service files --op read
$H ledger rule --ledger ledger --admin alice.key --name "staff data" --decision allow \
    --service files --op read --group staff --root data
```

See the [policy design](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-policy.md).

Admin grants follow a paglet to hosts covered by their host selector and
are materialized there. A grant derived by a host from an allow rule
covers only that host: after a move the paglet asks again, and the
destination evaluates its policy.

## Module trust

Signers vouch for modules. Admins decide which modules may run as roaming,
resident or system paglets. `module-policy trusted` makes roaming modules
need trust as well. Hosts end the paglets whose module loses trust.

```bash
$H keys init --role signer --name build-bot --out build-bot.key
$H ledger sign-module --ledger ledger --key build-bot.key --name calc --version 1.2 calc.wasm
$H ledger trust --ledger ledger --admin alice.key --name "lab apps" --class roaming --signer <signer-key-id>
$H ledger module-policy --ledger ledger --admin alice.key trusted
$H ledger revoke --ledger ledger --admin alice.key --module calc.wasm --reason vulnerable
```

See the [module design](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-modules.md).

## Data residency

A paglet that reads from a named root carries that root's mark for good. A
residency rule decides where a marked paglet may go:

- `host-only` keeps it on its current host when the rule is enforced
  (the host where it read, unless the rule took effect after a later move);
- `hosts` lets it move only to some hosts, chosen by name, key ID or
  `--host-label`.

```bash
$H ledger residency --ledger ledger --admin alice.key clinic hosts --host-label site
$H ledger residency --ledger ledger --admin alice.key vault host-only
```

Hosts enforce the rule when a move starts, and again when they choose the
candidates for a transfer ticket. Artifacts made from marked data carry the
mark as well, and so do files shared with `files.share`: the host that holds
such content gives it only to hosts the rule allows (see
[Moving content between hosts](system-paglets.md#moving-content-between-hosts)). See the
[residency design](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-residency.md).

## Running hosts

`paglets-host serve` runs a host of the mesh with its own copy of the
ledger. Hosts find each other through any enrolled host they join. They
exchange signed host announcements by gossip and send multicast beacons on
the local network. Each host keeps a registry of the others, with their
addresses, online state and versions, and refuses hosts that speak another
mesh protocol or paglet ABI.

```bash
$H keys init --role owner --name olga --out olga.key          # prints <olga-key-id>
$H keys init --role host --name lab-2 --out lab-2.key         # prints <lab-2-key-id>
$H keys init --role host --name laptop --out laptop.key       # prints <laptop-key-id>
$H ledger enroll --ledger ledger --admin alice.key host <lab-2-key-id> lab-2   # lab-1 is enrolled above
$H ledger enroll --ledger ledger --admin alice.key host <laptop-key-id> laptop
$H ledger enroll --ledger ledger --admin alice.key owner <olga-key-id> olga --group staff
cp -r ledger ledger-lab-1 && cp -r ledger ledger-lab-2 && cp -r ledger ledger-laptop
$H serve --key lab-1.key --ledger ledger-lab-1 --state state-1 --listen 0.0.0.0:7443 \
    --advertise https://lab-1:7443 &
$H serve --key lab-2.key --ledger ledger-lab-2 --state state-2 --listen 0.0.0.0:7443 \
    --advertise https://lab-2:7443 --join https://lab-1:7443 &
$H serve --key laptop.key --ledger ledger-laptop --state state-3 --no-listen \
    --join https://lab-1:7443 &                                    # behind NAT: reached through relays
```

Each host gets its own copy of the ledger. In practice the hosts run on
different machines, and each copy is made there from the admin's ledger.
Records an admin adds to a local ledger later reach a live host with
`remote push` (`remote approve` and `remote deny` push their decision
themselves). The hosts then spread them by gossip.

> [!NOTE]
> `serve` names its state directory with `--state`, while `run`, `list`
> and `call` use `--state-dir`. The two layouts differ as well, so do not
> point both at the same directory. See
> [Configuration and troubleshooting](configuration.md#state-directories).

A host listens on `127.0.0.1` and a free port unless `--listen` says
otherwise, so hosts on other machines need `--listen 0.0.0.0:PORT` (or a
specific address). `--advertise` sets the URL that other hosts use to reach
it; give it whenever the listening address is not reachable as it is.

A host whose key is not enrolled yet still starts. It treats the hosts it
joins or knows as seeds, and waits for an admin's enrollment record to
arrive by gossip. See
[enrollment-pending hosts](configuration.md#a-host-is-not-enrolled-yet).

Hosts talk over HTTPS. Inside it they use end-to-end encrypted, mutually
authenticated Noise channels. A host without inbound ports (`--no-listen`)
polls a few reachable hosts that relay for it. Everybody else reaches it
through channels tunneled through those relays, still encrypted end to end.

## Review status

The Noise channel, record and passport signatures, key handling and worker
sandboxes have internal tests, including platform checks. They have not
had an independent external security review. The
[hardening plan](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-hardening.md)
tracks the pending cryptographic, key/sandbox and threat-model reviews.

## TLS certificates

TLS keeps reverse proxies and firewalls working. It does not authenticate
hosts: the Noise channel inside it proves every host's key against the
ledger. Therefore the defaults are:

- Without `--tls-cert` and `--tls-key`, a host makes a self-signed
  certificate when it starts: a new P-256 key, the subject and issuer
  `CN=paglets host <first 16 digits of the key ID>`, no subject alternative
  names, valid from one hour before the start for 3650 days. It is kept in
  memory only, so each start makes a new one.
- Without `--tls-ca`, a host does not check the certificates of the hosts
  it connects to. It accepts any certificate, also an expired one or one
  for another name.

To use your own certificate, give both PEM files. The certificate must
name the host as the other hosts reach it, that is, the host name or IP
address of its `--advertise` URL, as a subject alternative name (a DNS
name, or an IP address for a URL with an IP address). A test CA and a
certificate for `lab-1.example.org` can be made with OpenSSL (in `bash`):

```bash
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 \
    -keyout lab-ca.key -out lab-ca.pem -subj "/CN=lab CA"
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -keyout lab-1.key.pem -out lab-1.csr -subj "/CN=lab-1.example.org"
openssl x509 -req -in lab-1.csr -CA lab-ca.pem -CAkey lab-ca.key -CAcreateserial -days 825 \
    -out lab-1.crt.pem -extfile <(printf "subjectAltName=DNS:lab-1.example.org\nextendedKeyUsage=serverAuth\n")
```

```bash
$H serve --key lab-1.key --ledger ledger-lab-1 --state state-1 --listen 0.0.0.0:7443 \
    --advertise https://lab-1.example.org:7443 \
    --tls-cert lab-1.crt.pem --tls-key lab-1.key.pem --tls-ca lab-ca.pem
openssl s_client -connect lab-1.example.org:7443 -CAfile lab-ca.pem \
    -verify_hostname lab-1.example.org </dev/null    # Verify return code: 0 (ok)
```

- The certificate file may hold intermediate CA certificates after the
  host's certificate. The host sends them along, so the hosts that check
  need only the root CA.
- The key must not be encrypted.
- `serve` refuses to start when it cannot use the files, for example with
  `paglets-host: the TLS key does not belong to the TLS certificate` or
  `a TLS certificate needs both the certificate and the key file`.
- A certificate that has expired or is not valid yet still starts the
  host, with a warning:
  `[net] TLS certificate lab-1.crt.pem: expired on <date>; hosts that check certificates (--tls-ca) refuse it`.

`--tls-ca FILE` makes the host check the certificate of every host it
connects to: for frames, gossip, `--join` contacts, and the relays that a
`--no-listen` host polls. The certificate must chain to a CA certificate
in `FILE`, be valid now, and name the host as in its URL. Only the
certificates in `FILE` count; the system's CA store is not used. `serve`
does not start when `FILE` cannot be read or holds no PEM certificate
(`paglets-host: cannot read the TLS CA file FILE`).

A host that fails the check is unreachable for the hosts that check. They
drop its frames and log the first failure with the reason, then a
summary at most once a minute, and a line when frames get through again:

```text
[net] frames to 35115e1c0c962d6e dropped: opening a channel to https://lab-3:7443: HTTP error: SSL server verification failed (unable to get local issuer certificate)
```

Other reasons are `(certificate has expired)` and, for a certificate
without the host's name, `SSL server hostname verification failed`. The
check only covers the connections that the checking host opens. A refused
host that does not check itself still delivers its frames, so the
checking hosts list it as `one-way` in `remote hosts`, with the reason
(`send failed: ...`). Requests that need an answer from them fail, for
example a move to a checking host
(`the move failed, the paglet stays: no host took the paglet; host <id>: no answer in time`).
Give `--tls-ca` to every host of the mesh or to none, and a certificate
from that CA to every host that listens.

> [!NOTE]
> `paglets-host remote` does not check TLS certificates and has no CA
> option. It relies on the Noise channel: it accepts only a host that is
> enrolled in its ledger copy (or the key given with `--host-key`), so a
> TLS interceptor can block a session but cannot read it or pose as the
> host. `--web-ca` is unrelated to the mesh: it adds CA certificates for
> the HTTPS sites that the `web` gateway fetches.

Design documents:

- [networking](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-networking.md)
- [mesh discovery](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-mesh.md)
- [relays](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-relay.md)

## Working with live hosts

`paglets-host remote` opens an end-to-end channel to a host, as an admin
or as an owner:

```bash
$H remote hosts --connect https://lab-2:7443 --key alice.key --ledger ledger
$H remote launch --connect https://lab-1:7443 --key olga.key --ledger ledger \
    build/linux-gcc16/guests/counter.wasm                          # prints the paglet ID
$H remote call --connect https://lab-1:7443 --key olga.key --ledger ledger <paglet-id> \
    increment '{"by": 5, "note": "first"}'
$H remote dispatch --connect https://lab-1:7443 --key olga.key --ledger ledger <paglet-id> lab-2
$H remote locate --connect https://lab-1:7443 --key olga.key --ledger ledger <paglet-id>
$H remote pin --connect https://lab-1:7443 --key olga.key --ledger ledger <paglet-id> --minutes 30
$H remote status --connect https://lab-2:7443 --key alice.key --ledger ledger
```

The module travels with a `launch` request (or `launch` names the hash of
a module the host already has). `send` delivers a one-way message, and
`describe` prints a paglet's [interface description](interface-descriptions.md):
the messages it understands and their types.

```bash
$H remote send --connect https://lab-1:7443 --key olga.key --ledger ledger <paglet-id> \
    increment '{"by": 1}' --priority 5
$H remote describe --connect https://lab-1:7443 --key olga.key --ledger ledger <paglet-id>
```

`dispatch` takes a transfer
ticket, which names the destination in one of these forms:

- a host name, a full key ID, or an unambiguous key-ID prefix of at least
  8 hexadecimal digits;
- `label:<label>`;
- `offer:<service>[.<op>]`: a host that offers the service;
- `any`.

A full, lowercase 64-digit key ID selects only that enrolled host, even
when another host uses the ID as its name. An unknown full ID never falls
back to a name. Names and short key IDs must identify exactly one enrolled
host across both forms; ambiguous addresses fail before placement filters
are applied. Use a full key ID to resolve ambiguity.

A ticket can add `?retries=N&arrival=active|inactive`. `retries` is 1 to 16,
default 3; arrival defaults to `active`.

`remote dispatch` waits until the move ended, at most 120 s. It prints
`moved to lab-2 <key-id>`, or exits with status 1 and the reason, for
example `the move failed, the paglet stays: no enrolled host matches label:gpu`.
If hosts match but all are excluded, the reason starts with
`every host matching label:gpu was excluded` and lists their reasons.
If eligible hosts are tried but none takes the paglet, the final report
also retains excluded hosts' reasons and names eligible hosts not tried
because of the ticket's retry limit. The combined diagnostic is limited
to 8 KB; oversized or excess reasons are counted as omitted.
A `call` accepted while the paglet is moving waits for the move as well
(up to 30 s). After the paglet has left, `remote call` and `remote send`
require a connection to its current host. Use `remote locate` to find it.

`remote hosts` shows each enrolled host with its state:

| State | Meaning |
|---|---|
| `this host` | the host the session is connected to |
| `online` | heard from recently, and this host's frames reach it |
| `one-way` | heard from recently, but this host's frames to it fail; `send failed:` gives the reason, for example a refused TLS certificate |
| `offline` | not heard from recently |

Admin work against a live host:

| Command | Does |
|---|---|
| `remote push` | sends the local ledger records to the host |
| `remote pull` | fetches the host's ledger records into the local ledger |
| `remote requests` | lists pending enrollment and grant requests |
| `remote approve`, `remote deny` | decides a request on the host |
| `remote audit` | shows the host's audit log |
| `remote modules` | lists the host's modules |
| `remote push-module` | stores a module on the host |
| `remote dispose` | ends a paglet |
| `remote terminate` | ends a paglet as failed at once (admins) |
| `remote pins`, `remote unpin` | lists pins, and releases one (admins) |
| `remote landscape` | lists every host with its labels, load and offers |
| `remote slots` | shows the compute slots and queues of the hosts |

> [!NOTE]
> [`paglets-web`](web-ui.md) shows the same in the browser: every host
> with its load and paglets, forms for the messages paglets describe, and
> next to each view the `remote` command that does the same.

Each host caches pages it has seen, with a default budget of 256 MB. A
returning paglet transfers only pages the destination no longer has; cache
eviction can require unchanged pages to be sent again. Embedders adjust
this with `Node::set_page_cache_bytes`.

Memory requirements default to unknown, with no operator cap for paglets
or jobs. Wasm32 supports up to 4 GB per instance; `serve --memory-mb N`
opts into a paglet ceiling, with separate job policy options. Growth beyond it fails; an
unhandled allocation failure or abort ends the paglet as `failed`. See
[allocation and image limits](configuration.md#limits).

## Location and pins

Location records live on up to three hosts chosen by consistent hashing
among the hosts currently considered up. A move waits for a majority of
those responsible hosts to record the new location before committing it.
The responsible set changes as hosts go down and return. Returning hosts
retain cached records until a newer move counter replaces them or the
records expire (one hour without refresh), and forward them to hosts that
are now responsible.

`remote locate` and the `locator` system paglet first use a local holder
record when the paglet is here. Otherwise they ask the responsible hosts
for their newest move counter. A majority answer with a record completes
that phase. At its deadline, even fewer answers can supply the best record
received. With no record, the lookup asks every live host who holds the
paglet; an unanswered second phase ends with `not_found`. Each phase has a
default 3 s deadline, so an unsuccessful lookup can take about 6 s, plus
host scheduling and transport delays. Known-empty answers can advance it
earlier. Message routing also keeps a 10 s location cache and forwarding
tombstones after a move.

A pin keeps a paglet on its current host. The authorization and duration
depend on how it is requested:

| Path | Who may pin | Default request and limits |
|---|---|---|
| `remote pin` owner/admin session | the owner of the target paglet, or an admin; no `locator` policy rule required | 10 min by default, clamped to 1 s through 24 h |
| `locator.locate_and_pin` from a paglet | an endpoint with the `pin` right or `*`, and an `allow` rule for service `locator`, operation `pin` | 60 s by default; request clamped to 1 s through 30 days, then capped by the rule's duration maximum, or 1 h when that maximum is absent |

Pins expire automatically. Their capability can release them, and admins
can force release. A moving target can redirect pin requests up to eight
times. A busy target permits up to 40 waits of 100 ms, with new lookups
between waits; target timeouts and redirects add further delay. The whole
operation can exceed 4 s. Set request timeouts for lookup and pinning
together.

| Timing or limit | Default | Configuration |
|---|---|---|
| Heartbeat to enrolled hosts | 2 s | `Node::LocationTiming::heartbeat` |
| Host considered down after silence | 10 s | `host_timeout` |
| Holder record refresh | 5 min | `refresh` |
| Unrefreshed record expiry | 1 h | `record_ttl` |
| Each lookup phase deadline | 3 s | `lookup_timeout` |
| Message-routing location cache | 10 s | compiled in |
| Forwarding tombstone lifetime | 10 min | `runtime::Config::tombstone_ttl` |
| Message forwarding bound | 4 | compiled in |

An embedder changes the five `LocationTiming` fields through
`Node::set_location_timing`; `serve` has no switches for them. See the
[location design](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-location.md).

## Limits

A host keeps one owner's paglets from taking it over:

- at most 10 000 paglets of one owner per host. Beyond that, creating,
  cloning and moving paglets there fails with `quota`, and a paglet that
  cannot arrive stays where it was;
- at most 16 children and clones per handler call;
- at most 64 pins on one paglet;
- an optional per-handler watchdog, off unless the operator enables it;
- a 16 MB storage quota per paglet;
- messages of at most 1 MB.

A clone bomb therefore stops at the owner's limit, and other owners'
paglets keep running. These limits are compiled-in defaults: no
`paglets-host` option changes the count/storage defaults; memory and time policies
are opt-in host options (see [resource policy](long-running-tasks.md)).
[Configuration and troubleshooting](configuration.md#limits) names where
each one is defined. The
[hardening design](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-hardening.md)
lists every limit.
