# System paglets

Paglets run in a sandbox, so they cannot open files, create sockets or
start programs. Hosts lend them those resources through **system
paglets**: native services built into the host. Paglets call them by
message, through typed clients generated from the contracts in
`cpp/common/include/paglets/services/`.

A paglet gets a service endpoint in one of two ways:

- `paglets::service(name)` returns the host's default endpoint;
- the patterns helper `endpoint(name, next)` also asks the `directory`
  service.

Resource access is decided by the mesh policy. For example, a paglet asks
`grants` for a directory, and only gets one if a rule allows it or an
admin approves the request.

## Services

| Service | Endpoint | Operations | Purpose |
|---|---|---|---|
| `files` | directory | `list`, `stat`, `find`, `read`, `write`, `mkdir`, `move`, `remove`, `hash`, `share`, `save` | files inside a directory capability; the capability's rights limit what is possible. `hash` gives a file's SHA-256 without reading it into the paglet; `share` and `save` move files of any size between hosts |
| `grants` | default (mesh) | `request`, `status`, `release`, `list` | ask the mesh policy for a resource, such as a directory of a named root; requested durations are clamped to 1 s through 30 days, then limited by the matching rule; `status` reports `granted` only while an approval remains active and unexpired |
| `server-info` | directory, policy-narrowed | `summary`, `load`, `volumes`, `processes` | the host's system, load, volumes and processes, in one schema on every platform |
| `directory` | default | `lookup`, `publish`, `unpublish`, `list` | finds system services and paglets that published themselves under a name |
| `storage` | default | `get`, `put`, `remove`, `list` | host-local persistent key-value storage within a quota; survives local deactivation and restart with a state directory, removed when a move commits; a clone starts empty |
| `artifacts` | directory | `put`, `get`, `stat` | content-addressed blobs (SHA-256), for results larger than a message; artifact capabilities work on every host |
| `pubsub` | directory | `create`, `publish`, `subscribe`, `unsubscribe` | topics on a host |
| `user-info` | default | `notify` | notifications to the owner of a paglet |
| `locator` | default (mesh) | `locate`, `locate_and_pin`, `release` | find a paglet anywhere in the mesh and pin it |
| `mesh-info` | default (mesh) | `snapshot`, `landscape`, `select`, `find_offers` | every host's snapshot (load, free slots, labels, offers, and its connections to the other hosts: up or down, since when, why frames fail), sent directly to every live host |
| `compute-slots` | default (mesh) | `request_slot`, `release_slot`, `status`, `candidates` | admission of compute work on a host, with FIFO lease grants and shared priority with native jobs; later work preserves the older head's required capacity; repeating a queued request retains its position; repeating an allocated job/core request renews its lease within the original lifetime cap; changed allocation parameters require releasing the old lease; an oversized request first tries a known peer with enough free slots and is rejected if none is suitable; reducing host capacity rejects impossible waiters with `compute.rejected` |
| `zombies` | default (mesh) | `join`, `peers`, `trace`, `ended`, `failed`, `state`, `deliver`, `result`, `cancel`, `redirect`, `list`, `traces` | the host's registry of zombie groups ([The zombie protocol](zombie-protocol.md)): one replica per group and host, tombstones and sink redirects that reach every host, the replicas' protocol traces |
| `web` | directory, with `--web` | `capabilities`, `fetch`, `extract_text`, `download`, `search` | mediated web access, on hosts started with `--web` |
| `ai` | directory, with `--ai` | `capabilities`, `summarize`, `classify`, `extract`, `generate`, `embed`, `describe_image` | local inference, on hosts started with `--ai` |

Default endpoints appear in `self_info().services`; `paglets::service`
reads that list. Directory endpoints require a `directory.lookup` call
(or the patterns `endpoint` helper). Mesh-only services are installed by
the mesh node and need not exist in a minimal embedded runtime.

A published paglet name is visible to paglets of the publisher's owner.
Publishing it with `is_public` makes it visible to every owner. The same
visibility rule applies to lookup and listing.

Services operating on the caller's own state or lent capabilities need no
ambient policy rule. `server-info` has ambient authority: on a mesh, its
directory lookup returns only allowed operations and returns `denied` if
none are allowed. `web` and `ai` instead check policy per request, against
the URL host/path or operation/model. A service endpoint does not bypass
the rights of a lent directory or artifact capability.

The
[system paglets design](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-system-paglets.md)
and the
[compute design](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-compute.md)
describe the services in detail.

`mesh-info.select` returns matching hosts first. If none fit its resource
and label requirements, it returns the least loaded hosts with `fallback`
set. Callers must inspect that flag when the requirements are mandatory;
selection is advice, not admission to compute slots or compute jobs. The
[Pi example](examples/pi.md) submits compute jobs directly to available
hosts rather than using the older select-and-worker-paglet approach.

## Compute-slot leases

By default a lease lasts the greater of 10 minutes and twice its positive
`estimated_ms`, capped at 24 hours. A nonpositive estimate uses 10 minutes.
Repeating the same job and core request renews the lease without shortening
it, but cannot pass the continuous deadline fixed at the original grant.
An expired request rejoins normal admission. Waiting in the queue does not
spend lease lifetime, and updating an estimate keeps the queue position.

`run_now` decisions and status leases report `expires_ms` in Unix
milliseconds; status also retains the original `granted_ms`. The host
enforces expiry with a monotonic clock. Expiry releases the slots and sends
`compute.expired`: a rejected Decision containing the old lease ID, core
count and expiry. Releasing that expired ID returns `released=false`.

Leases account for cooperative work; they do not preempt CPU execution.
Native compute jobs have their own enforced running-time limits.
Leases and queues are transient: ask again after a host restart or arrival
on another host. Host embedders can tune `Node::ComputeTiming::lease_base`
and `lease_cap`, normalized to 1 ms through 24 hours with cap at least base.
Timing changes affect new grants and renewal extensions; an existing
lease's original continuous deadline remains fixed.

## Background file scans

`files.scan_start`, `files.scan_next` and `files.scan_cancel` implement the
framework's service-task protocol. Applications normally use
`patterns::scan_files`, which returns a `tasks::TaskHandle` and drives the
protocol, progress, backpressure, cancellation and deadlines automatically.
The native filesystem traversal runs on a two-thread background pool;
each batch is bounded, and a scan can take minutes while its paglet keeps
answering messages. Scan ownership and directory authority are checked by
the host, and ending a paglet cancels its scans. Cursors stay on the host
and are not durable across restarts.

`files.find` remains a one-reply compatibility operation, now also running
in the background. It returns at most 10000 entries and 512 KB with a
`truncated` flag. Its caller's request timeout still applies; use scan tasks
for large or slow trees. See [Long-running agents and tasks](long-running-tasks.md)
for the common API and lifecycle, and [Mesh File Finder](examples/file-finder.md)
for a complete scan with bounded returned samples.

## Moving content between hosts

A paglet moves as a memory image (within its declared/host allowance), so it should avoid carrying
large data in its memory. Instead, it carries **artifact capabilities**: a
capability names its content by SHA-256 and names the host that holds it,
together with a token that this host made. The capability is a few bytes.
It moves with the paglet and travels in messages to paglets on other
hosts. The content goes directly from host to host, and only when a host
needs it.

- `files.share` makes a file of a directory capability (right `read`)
  available to the mesh. The host hashes the file and replies with an
  artifact capability. Nothing is copied and nothing is read into the
  paglet. The host serves the file from where it is, as long as the file
  keeps its size and modification time.
- `files.save` writes the content of an artifact capability to a file
  (rights `create`, and `write` to replace a file). The host fetches the
  content from the host that holds it, in parts of 1 MB, writes it beside
  the target, checks its SHA-256, and only then renames it into place. A
  file appears whole or not at all. The parent directory must exist.
- `artifacts.get` and `artifacts.stat` work on every host. A host that
  lacks the content first fetches it into its own `artifacts` store.
- Artifacts that `artifacts.put` and the `web` gateway's `download` create
  work the same way.

The guest SDK wraps the two file operations in `share_file` and
`save_file` (see [Writing paglets](writing-paglets.md#the-patterns-library)):

```cpp
namespace pt = paglets::patterns;

// On the source host, with a `dir` capability that has the right `read`:
pt::share_file(dir, "results/run-7.h5", [this](paglets::Result<pt::SharedFile> f) {
    if (f) shared_ = std::move(*f);  // name, sha256, size and the capability
});

// Later, on the destination host, with a `dir` capability that has `create`:
pt::save_file(dir, "inbox/run-7.h5", shared_.content,
              [](paglets::Result<paglets::services::files::Entry> e) { /* ... */ });
```

The host that holds the content serves it only:

- to enrolled hosts of the mesh;
- with the token it made for that content;
- within the data residency of the content. A shared file carries the mark
  of its root (`root:<name>`), and an artifact carries the marks of the
  paglets that stored it. A host that the residency rule does not allow
  gets `denied`, and the destination host writes nothing.

The paglet that shares a file is not marked, because it reads nothing. A
paglet that reads content with `artifacts.get` takes the content's marks,
as it does when it reads a file. Listing or finding files of a root still
marks the paglet, because file names are content of the root as well.

> [!NOTE]
> Hosts keep fetched artifacts in their `artifacts` store, within its limit
> (1 GB by default). `save` writes directly into the destination
> directory, so it needs no space in the store. Hosts remember shared files
> across restarts. A shared file that changes is no longer served; share
> it again to give out its new content.

See the
[content exchange design](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-content-exchange.md).

## Service offers

Hosts announce what their system paglets can do as **offers**. An offer is
a service and an operation, with attributes such as a model name or a
size limit. `mesh-info` spreads the offers with each host's snapshot:

- paglets find offers with `find_offers`;
- paglets move to an offer with a transfer ticket such as
  `offer:ai.summarize`;
- admins see every host's offers with `remote landscape`.

## Gateways to the web and to AI

Gateway hosts offer `web` or `ai`. Paglets use them within the policy
rules for the services `web` and `ai`. In this example, `gw.key` and
`mac.key` are host keys enrolled, and their ledger copies made, as for
`lab-2` in [Running hosts](hosts-and-meshes.md#running-hosts).

```bash
H=build/linux-gcc16/host/paglets-host
$H serve --key gw.key --ledger ledger-gw --state state-gw --web --join https://lab-1:7443 &
$H serve --key mac.key --ledger ledger-mac --state state-mac --ai ollama --ai-model llama3.2 \
    --join https://lab-1:7443 &
$H ledger rule --ledger ledger --admin alice.key --name downloads --decision allow \
    --service web --op download --root example.org --path "files/**"
```

`web` checks every URL against the policy:

- It refuses internal addresses (loopback, private, link-local, and the
  like). `--web-internal URL-PREFIX` allows chosen internal destinations,
  and `--web-no-internet` limits the host to them.
- It connects to the address it checked, so a second DNS answer cannot
  redirect the request.
- It checks every redirect again.
- It uses the proxy from `HTTPS_PROXY`, or `--web-proxy`. `--web-search`
  names a SearXNG instance for `search`.
- It checks the certificates of HTTPS sites against the system's CA
  certificates. `--web-ca FILE` adds CA certificates, for example of an
  internal CA; the system's CAs stay trusted. A refused certificate fails
  the request with a reason, such as
  `SSL server verification failed (unable to get local issuer certificate)`.

`download` stores the file as an artifact. The artifact capability works
on every host, so a paglet can carry it home, where its home host fetches
the content (see [Moving content between hosts](#moving-content-between-hosts)).

`ai` answers through Ollama (`--ai-url`, `--ai-model`). The `test` backend
gives fixed answers, which the test suite uses. Both gateways run their
work on a thread pool and reply later, so slow requests do not block the
host.

> [!NOTE]
> A residency mark also applies to gateways. A paglet that read data that
> must stay on site can only reach an `ai` host that the residency rule
> allows.

See the
[web and AI design](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-web-ai.md).

## Compute jobs

[Compute jobs](compute-jobs.md) are ABI imports backed by the runtime and
mesh, not another endpoint in the system-paglet table. Mesh policy uses
service `compute-jobs`, operation `submit`, to admit work and cap duration.
Running jobs share the host's compute-slot accounting; status and admin
cancellation are available through `paglets-host remote jobs`. Artifact
services carry input and output over the message-size limit.
