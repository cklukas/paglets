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

More ledger commands:

- `enroll` and `remove` add and remove members directly;
- `deny` rejects a request;
- `admins` adds or removes admins and changes the quorum;
- `sign` co-signs a record that needs several admins.

The [command-line reference](cli-reference.md#ledger) lists every ledger
command and option. The
[ledger design](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-ledger.md)
describes the records and their signatures.

## Policy

Rules decide whether a request for a host resource is **allowed**, **asked**
or **denied**. Paglets ask the `grants` system paglet. Admins approve or
deny the asked requests from any host. Every decision lands in the audit
log.

```bash
$H ledger rule --ledger ledger --admin alice.key --name "staff docs" --decision ask \
    --service files --op read --group staff --root data --path "docs/**"
$H ledger show --ledger ledger                     # rules, grants, pending grant requests
$H ledger approve --ledger ledger --admin alice.key <grant-request-id>
$H ledger audit --ledger ledger
```

See the [policy design](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-policy.md).

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

- `host-only` keeps it on the host where it read;
- `hosts` lets it move only to some hosts, chosen by name, key ID or
  `--host-label`.

```bash
$H ledger residency --ledger ledger --admin alice.key clinic hosts --host-label site
$H ledger residency --ledger ledger --admin alice.key vault host-only
```

Hosts enforce the rule when a move starts, and again when they choose the
candidates for a transfer ticket. Artifacts made from marked data carry the
mark as well. See the
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
$H ledger enroll --ledger ledger --admin alice.key owner <olga-key-id> olga
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

The module travels with a `launch` request. `dispatch` takes a transfer
ticket, which names the destination in one of these forms:

- a host name or key ID;
- `label:<label>`;
- `offer:<service>[.<op>]`: a host that offers the service;
- `any`.

A ticket can add `?retries=N&arrival=active|inactive`.

`remote dispatch` waits until the move ended, at most 120 s. It prints
`moved to lab-2 <key-id>`, or exits with status 1 and the reason, for
example `the move failed, the paglet stays: no host matches label:gpu`.
A `call` to a paglet that is moving waits for the move as well (up to
30 s).

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
| `remote pins`, `remote unpin` | lists pins, and releases one (admins) |
| `remote landscape` | lists every host with its labels, load and offers |
| `remote slots` | shows the compute slots and queues of the hosts |

## Location and pins

Location records live on hosts chosen by consistent hashing. A majority of
them is updated on every move, and they fail over when hosts go down. So a
paglet can be found from any host (`remote locate`, or the `locator` system
paglet). A pin keeps a paglet on its host for a while. Pinning needs a
policy rule for service `locator`, operation `pin`. See the
[location design](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-location.md).

## Limits

A host keeps one owner's paglets from taking it over:

- at most 10 000 paglets of one owner per host. Beyond that, creating,
  cloning and moving paglets there fails with `quota`, and a paglet that
  cannot arrive stays where it was;
- at most 16 children and clones per handler call;
- at most 64 pins on one paglet;
- at most 5 s per handler;
- a 16 MB storage quota per paglet;
- messages of at most 1 MB.

A clone bomb therefore stops at the owner's limit, and other owners'
paglets keep running. These limits are compiled-in defaults: no
`paglets-host` option changes them.
[Configuration and troubleshooting](configuration.md#limits) names where
each one is defined. The
[hardening design](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-hardening.md)
lists every limit.
