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

| Service | Operations | Purpose |
|---|---|---|
| `files` | `list`, `stat`, `find`, `read`, `write`, `mkdir`, `move`, `remove` | files inside a directory capability; the capability's rights limit what is possible |
| `grants` | `request`, `status`, `release`, `list` | ask the mesh policy for a resource, such as a directory of a named root |
| `server-info` | `summary`, `load`, `volumes`, `processes` | the host's system, load, volumes and processes, in one schema on every platform |
| `directory` | `lookup`, `publish`, `unpublish`, `list` | finds system services and paglets that published themselves under a name |
| `storage` | `get`, `put`, `remove`, `list` | persistent key-value storage of each paglet, within a quota |
| `artifacts` | `put`, `get`, `stat` | content-addressed blobs (SHA-256), for results larger than a message |
| `pubsub` | `create`, `publish`, `subscribe`, `unsubscribe` | topics on a host |
| `user-info` | `notify` | notifications to the owner of a paglet |
| `locator` | `locate`, `locate_and_pin`, `release` | find a paglet anywhere in the mesh and pin it |
| `mesh-info` | `snapshot`, `landscape`, `select`, `find_offers` | every host's snapshot (load, free slots, labels, offers), spread by gossip |
| `compute-slots` | `request_slot`, `release_slot`, `status`, `candidates` | admission of compute work on a host, with a queue and redirects to hosts with free slots |
| `web` | `capabilities`, `fetch`, `extract_text`, `download`, `search` | mediated web access, on hosts started with `--web` |
| `ai` | `capabilities`, `summarize`, `classify`, `extract`, `generate`, `embed`, `describe_image` | local inference, on hosts started with `--ai` |

The
[system paglets design](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-system-paglets.md)
and the
[compute design](https://github.com/cklukas/paglets/blob/cpp/planning/cpp-compute.md)
describe the services in detail.

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
rules for the services `web` and `ai`.

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

`download` stores the file as an artifact.

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
