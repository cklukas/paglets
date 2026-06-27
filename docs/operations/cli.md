# Paglets CLI

Paglets exposes one Typer-based command entry point:

```bash
uv run paglets --help
```

Use `paglets` for host startup, mesh/resource inspection, compute job
operations, artifact recovery, distributed search, and packaged examples.

## Shell Completion

Typer provides completion installers for supported shells:

```bash
uv run paglets --install-completion
uv run paglets --show-completion
```

`--install-completion` installs completion for the detected shell.
`--show-completion` prints the completion script so it can be reviewed,
customized, or installed manually.

## Top-Level Commands

| Command | Purpose |
| --- | --- |
| `paglets host` | Start a Paglets host process. |
| `paglets sys` | Inspect CPU, memory, disk, and processes across the mesh. |
| `paglets mesh` | Inspect mesh snapshots and rank compute targets. |
| `paglets jobs` | Inspect and manage compute slots, queues, jobs, and groups. |
| `paglets artifacts` | List, inspect, download, and remove host artifacts. |
| `paglets search` | Search file names or file contents across mesh hosts. |
| `paglets examples` | Run packaged example workflows. |

Root options:

| Option | Meaning |
| --- | --- |
| `--no-color` | Disable colors and styled terminal output. |
| `--install-completion` | Install shell completion for the current shell. |
| `--show-completion` | Print the completion script for the current shell. |
| `--help` | Show help. |

Common options used by networked commands:

| Option | Meaning |
| --- | --- |
| `--entry NAME` | Use a discovered entry host by name. If omitted, Paglets selects a reachable entry host. |
| `--timeout SECONDS` | HTTP or mesh operation timeout. |
| `--api-key-env NAME` | Read the bearer API key from a named environment variable instead of the default. |
| `--json` | Print JSON for commands that support machine-readable output. |

## Host

Start a host:

```bash
uv run paglets host --name alpha --port 8765 --mesh-version dev
```

| Option | Meaning |
| --- | --- |
| `--name`, `-n` | Required host/context name, such as `alpha`. |
| `--host HOST` | Bind host. Defaults to `127.0.0.1`. |
| `--bind-public HOST` | Bind to a public or LAN address. Use `auto` for detected LAN IP; repeat for multiple addresses. |
| `--port`, `-p` | Bind port. Defaults to `8765`. |
| `--peer URL` | Peer host URL to join; repeatable. |
| `--mesh` / `--no-mesh` | Enable or disable host mesh discovery. |
| `--mesh-multicast` / `--no-mesh-multicast` | Enable or disable UDP multicast beacons. |
| `--mesh-lan-discovery` / `--no-mesh-lan-discovery` | Enable or disable TCP LAN discovery. |
| `--public-url URL` | Externally reachable base URL. |
| `--connect-to URL` | Relay base URL for outbound connect mode. |
| `--relay-offline-after SECONDS` | Seconds before a relayed host is considered offline. |
| `--relay-delivery-timeout SECONDS` | Relayed delivery acknowledgment timeout. |
| `--relay-queue-limit COUNT` | Maximum queued relay deliveries per connected host. |
| `--api-key-env NAME` | API key environment variable. |
| `--mesh-version VALUE` | Override the mesh code-version gate. |
| `--tag TAG` | Advertise a host tag; repeatable. |
| `--property KEY=VALUE` | Advertise a host property; repeatable. |
| `--persistence-dir PATH` | Directory for durable inactive paglet storage. |
| `--persistent-storage-quota SIZE` | Per-class persistent storage quota, such as `10M`, or `none`. |
| `--artifact-max-size SIZE` | Maximum accepted artifact size, such as `1G`, or `none`. |
| `--artifact-storage-quota SIZE` | Total artifact storage quota, such as `10G`, or `none`. |
| `--artifact-spool-ttl SECONDS` | Artifact spool cleanup TTL. |
| `--launch-config PATH` | Launch config TOML path. |
| `--sync-launch-config` / `--no-sync-launch-config` | Copy or update the bundled launch config before startup. |
| `--yes`, `-y` | Accept launch config update prompts. |
| `--auto-update-from-git` | Run git update on startup and accept trusted mesh update requests. |

## System Inspection

Commands:

| Command | Purpose |
| --- | --- |
| `paglets sys summary` | Show compact host summaries. |
| `paglets sys load` | Show CPU, memory, swap, load average, and GPU information. |
| `paglets sys df [PATH ...]` | Show disk usage for all volumes or selected paths. |
| `paglets sys ps QUERY` | List matching processes. |

`summary` options:

| Option | Meaning |
| --- | --- |
| `--entry NAME` | Entry host name. |
| `--timeout SECONDS` | Seconds to wait for mesh replies. |
| `--json` | Print JSON. |
| `--api-key-env NAME` | API key environment variable. |

`load` options:

| Option | Meaning |
| --- | --- |
| `--entry NAME` | Entry host name. |
| `--timeout SECONDS` | Seconds to wait for mesh replies. |
| `--interval SECONDS` | CPU sampling interval per host. |
| `--gpu` / `--no-gpu` | Include or skip best-effort GPU lookup. |
| `--json` | Print JSON. |
| `--api-key-env NAME` | API key environment variable. |

`df` arguments and options:

| Argument or option | Meaning |
| --- | --- |
| `PATH ...` | Optional paths to inspect on every host. If omitted, all volumes are listed. |
| `--entry NAME` | Entry host name. |
| `--timeout SECONDS` | Seconds to wait for mesh replies. |
| `--json` | Print JSON. |
| `--api-key-env NAME` | API key environment variable. |

`ps` arguments and options:

| Argument or option | Meaning |
| --- | --- |
| `QUERY` | Case-insensitive process name or command-line search text. |
| `--entry NAME` | Entry host name. |
| `--timeout SECONDS` | Seconds to wait for mesh replies. |
| `--limit COUNT` | Maximum processes per host. |
| `--args` | Include process command lines. |
| `--json` | Print JSON. |
| `--api-key-env NAME` | API key environment variable. |

## Mesh

Commands:

| Command | Purpose |
| --- | --- |
| `paglets mesh summary` | Show known fresh mesh resource snapshots. |
| `paglets mesh targets` | Rank eligible compute targets. |

`summary` options:

| Option | Meaning |
| --- | --- |
| `--entry NAME` | Entry host name. |
| `--timeout SECONDS` | HTTP timeout. |
| `--max-age SECONDS` | Freshness cutoff; `0` uses the service default. |
| `--limit COUNT` | Maximum hosts to print; `0` prints all returned hosts. |
| `--json` | Print JSON. |
| `--api-key-env NAME` | API key environment variable. |

`targets` options:

| Option | Meaning |
| --- | --- |
| `--entry NAME` | Entry host name. |
| `--timeout SECONDS` | HTTP timeout. |
| `--limit COUNT` | Maximum targets to print. |
| `--max-age SECONDS` | Freshness cutoff; `0` uses the service default. |
| `--max-load-per-cpu VALUE` | Maximum one-minute load divided by logical CPUs. |
| `--max-cpu-percent VALUE` | Maximum sampled CPU percent. |
| `--mem SIZE` | Minimum available RAM, such as `512M`. |
| `--disk SIZE` | Minimum free work storage, such as `1G`. |
| `--include-self` / `--exclude-self` | Include or exclude the entry host. |
| `--json` | Print JSON. |
| `--api-key-env NAME` | API key environment variable. |

## Jobs

Commands:

| Command | Purpose |
| --- | --- |
| `paglets jobs status` | Show aggregate local scheduler capacity. |
| `paglets jobs queue` | Show queued slot requests. |
| `paglets jobs why` | Explain blockers for queued requests. |
| `paglets jobs top` | Show active job resource usage. |
| `paglets jobs ps` | List active and inactive compute job paglets. |
| `paglets jobs history` | Show recent finished job usage history. |
| `paglets jobs hosts` | Find candidate hosts for a resource request. |
| `paglets jobs groups` | Show compute job-group collectors. |
| `paglets jobs rm` | Remove queued requests and inactive waiting jobs. |

`status`, `queue`, `why`, and `top` options:

| Option | Meaning |
| --- | --- |
| `--entry NAME` | Entry host name. |
| `--timeout SECONDS` | HTTP timeout. |
| `--json` | Print JSON. |
| `--api-key-env NAME` | API key environment variable. |

`ps` options:

| Option | Meaning |
| --- | --- |
| `--entry NAME` | Entry host name. |
| `--timeout SECONDS` | HTTP timeout. |
| `--active` | Include active compute jobs. If used alone, inactive jobs are omitted. |
| `--inactive` | Include inactive compute jobs. If used alone, active jobs are omitted. |
| `--job ID` | Filter by compute job ID; repeatable. |
| `--agent ID` | Filter by agent ID; repeatable. |
| `--state STATE` | Filter by compute or application status; repeatable. |
| `--class NAME` | Filter by class name or class-name suffix; repeatable. |
| `--json` | Print JSON. |
| `--api-key-env NAME` | API key environment variable. |

If neither `--active` nor `--inactive` is supplied, `ps` lists both active and
inactive compute jobs.

`history` options:

| Option | Meaning |
| --- | --- |
| `--entry NAME` | Entry host name. |
| `--timeout SECONDS` | HTTP timeout. |
| `--limit COUNT` | Maximum finished jobs to print; `0` prints all retained records. |
| `--json` | Print JSON. |
| `--api-key-env NAME` | API key environment variable. |

`hosts` options:

| Option | Meaning |
| --- | --- |
| `--entry NAME` | Entry host name. |
| `--timeout SECONDS` | HTTP timeout. |
| `--limit COUNT` | Maximum candidates to print. |
| `--cores COUNT` | Requested CPU cores. |
| `--mem SIZE` | Requested RAM, such as `512M`. |
| `--disk SIZE` | Requested temp storage, such as `1G`. |
| `--gpu` | Require GPU support. |
| `--gpu-memory MB` | Requested GPU memory in MB. |
| `--tag TAG` | Require a host tag; repeatable. |
| `--prefer-tag TAG` | Prefer a host tag; repeatable. |
| `--exclude-tag TAG` | Reject hosts with this tag; repeatable. |
| `--exclude-host NAME_OR_URL` | Reject a host name or URL; repeatable. |
| `--json` | Print JSON. |
| `--api-key-env NAME` | API key environment variable. |

`groups` options:

| Option | Meaning |
| --- | --- |
| `--entry NAME` | Entry host name. |
| `--timeout SECONDS` | HTTP timeout. |
| `--group ID` | Restrict output to one group ID. |
| `--json` | Print JSON. |
| `--api-key-env NAME` | API key environment variable. |

`rm` options:

| Option | Meaning |
| --- | --- |
| `--job ID` | Remove queued or waiting jobs by job ID; repeatable. |
| `--agent ID` | Remove queued or waiting jobs by agent ID; repeatable. |
| `--all` | Match all queued and waiting jobs. |
| `--entry NAME` | Entry host name. |
| `--timeout SECONDS` | HTTP timeout. |
| `--state STATE` | Inactive job status to dispose; repeatable. Defaults to `WAITING_FOR_SLOT` for inactive job disposal. |
| `--dry-run`, `-n` | Preview matches without removing them. |
| `--force`, `-f` | Remove without prompting. |
| `--json` | Print JSON. |
| `--api-key-env NAME` | API key environment variable. |

`rm` removes matching queued scheduler requests and matching inactive waiting
compute job paglets. It does not terminate running job processes.

## Artifacts

Commands:

| Command | Purpose |
| --- | --- |
| `paglets artifacts list` | List artifacts on a host. |
| `paglets artifacts info ARTIFACT_ID` | Print artifact metadata as JSON. |
| `paglets artifacts get ARTIFACT_ID OUTPUT` | Download an artifact. |
| `paglets artifacts rm ARTIFACT_ID` | Delete an artifact. |

Shared artifact options:

| Option | Meaning |
| --- | --- |
| `--entry NAME` | Entry host name for discovery. |
| `--host URL` | Explicit host or relay URL. |
| `--timeout SECONDS` | Request timeout. |
| `--api-key-env NAME` | API key environment variable. |

Additional options:

| Command | Option | Meaning |
| --- | --- | --- |
| `list` | `--owner ID`, `--agent ID` | Filter artifacts by owner agent ID. |
| `list` | `--json` | Print JSON. |
| `get` | `--move` | Delete the source artifact after verified download. |
| `rm` | `--force`, `-f` | Delete without prompting. |
| `rm` | `--quiet`, `-q` | Do not print confirmation. |

## Search

Commands:

| Command | Purpose |
| --- | --- |
| `paglets search grep PATTERN [PATH ...]` | Search file contents. |
| `paglets search find [PATTERN] [PATH ...]` | Search file and directory names. |

Shared search options:

| Option | Meaning |
| --- | --- |
| `--entry NAME` | Entry host name. |
| `--host NAME_OR_URL` | Restrict search to a mesh host name or URL; repeatable. |
| `--timeout SECONDS` | Seconds to wait for mesh replies. |
| `--poll-interval SECONDS` | Seconds each drain call may wait for new events. |
| `--json` | Print final summary JSON. |
| `--jsonl` | Stream event JSON lines. |
| `--no-stream` | Buffer events and print them after completion. |
| `--api-key-env NAME` | API key environment variable. |
| `-i`, `--ignore-case` | Case-insensitive search. |
| `-S`, `--smart-case` | Case-insensitive unless the pattern has uppercase letters. |
| `-F`, `--fixed-strings` | Treat the pattern as a literal string. |
| `-w`, `--word-regexp` | Match whole words only. |
| `-g`, `--glob PATTERN` | Include or exclude paths; repeatable. |
| `-t`, `--type NAME` | Search only a supported file type; repeatable. |
| `--hidden` | Search hidden files and directories. |
| `--no-ignore` | Do not use ignore files. |

`grep` options:

| Option | Meaning |
| --- | --- |
| `-A`, `--after-context COUNT` | Print lines after each match. |
| `-B`, `--before-context COUNT` | Print lines before each match. |
| `-C`, `--context COUNT` | Print lines before and after each match. |
| `-n`, `--line-number` / `--no-line-number` | Print or suppress line numbers. |
| `-c`, `--count` | Print matching-line counts per file. |
| `-l`, `--files-with-matches` | Print only paths with matches. |

`find` options:

| Option | Meaning |
| --- | --- |
| `--full-path` | Match against the full path instead of the basename. |
| `-e`, `--extension EXT` | Limit to extension; repeatable. |
| `--kind any|file|dir|symlink` | Restrict emitted path kind. |

## Examples

Commands:

| Command | Purpose |
| --- | --- |
| `paglets examples pi` | Compute decimal Pi digits across the mesh. |
| `paglets examples analysis` | Start the synthetic distributed analysis example. |
| `paglets examples perf` | Run host performance benchmarks. |
| `paglets examples mesh-benchmark` | Measure directed mobile-agent travel times. |
| `paglets examples file push SOURCE` | Copy or move a file from the entry host to a remote host. |
| `paglets examples file pull SOURCE` | Copy or move a file from a remote host to the entry host. |

`pi` options:

| Option | Meaning |
| --- | --- |
| `--entry NAME` | Entry host name. |
| `--start OFFSET` | Zero-based decimal digit position after the point. |
| `--digits COUNT` | Number of decimal digits to compute. |
| `--batch-size COUNT` | Chudnovsky terms per worker batch. |
| `--max-in-flight COUNT` | Global in-flight batch cap; `0` uses free load slots. |
| `--max-workers-per-host COUNT` | Per-host worker cap; `0` uses free load slots. |
| `--timeout SECONDS` | Whole-job timeout; `0` disables it. |
| `--stream-chunk-size COUNT` | Maximum newly available decimal digits per text-mode poll. |
| `--request-timeout SECONDS` | HTTP request timeout for coordinator calls. |
| `--max-load-per-cpu VALUE` | Maximum one-minute load divided by logical CPUs. |
| `--max-cpu-percent VALUE` | Maximum sampled CPU percent. |
| `--mem SIZE` | Minimum available RAM, such as `512M`. |
| `--disk SIZE` | Minimum free work storage, such as `1G`. |
| `--json` | Print JSON. |
| `--api-key-env NAME` | API key environment variable. |

`analysis` options:

| Option | Meaning |
| --- | --- |
| `--entry NAME` | Entry or home host name. |
| `--tasks COUNT` | Number of analysis jobs. |
| `--db PATH` | SQLite result DB path on the home host. |
| `--rows COUNT` | Synthetic rows per job. |
| `--features COUNT` | Synthetic features per job. |
| `--trees COUNT` | Random forest tree count. |
| `--target-runtime SECONDS` | Minimum compute duration per job. |
| `--memory SIZE` | Requested RAM per job. |
| `--cpu-cores COUNT` | Requested logical CPU cores per job. |
| `--temp-storage SIZE` | Requested temp storage per job. |
| `--db-lock-timeout SECONDS` | Seconds to wait for the SQLite write lock. |
| `--wait SECONDS` | Seconds to wait for seeder completion. |
| `--api-key-env NAME` | API key environment variable. |

`perf` options:

| Option | Meaning |
| --- | --- |
| `--entry NAME` | Entry host name. |
| `--timeout SECONDS` | Seconds to wait for replies. |
| `--duration SECONDS` | Seconds per CPU/memory kernel. |
| `--disk-size SIZE` | Temporary file size per tested volume. |
| `--workers COUNT` | Multi-core worker count; `0` uses logical CPU count. |
| `--path PATH` | Disk path to benchmark; repeatable. |
| `--cpu` / `--no-cpu` | Run or skip CPU benchmarks. |
| `--memory` / `--no-memory` | Run or skip memory benchmarks. |
| `--disk` / `--no-disk` | Run or skip disk benchmarks. |
| `--lock-timeout SECONDS` | Seconds to wait for the local benchmark lock. |
| `--json` | Print JSON. |
| `--verbose` | Print skipped disk targets and diagnostics. |
| `--api-key-env NAME` | API key environment variable. |

`mesh-benchmark` options:

| Option | Meaning |
| --- | --- |
| `--entry NAME` | Entry host name. |
| `--timeout SECONDS` | Seconds to wait for completion. |
| `--repeats COUNT` | Repeat the directed mesh route this many times. |
| `--payload-size SIZE` | Random ASCII payload size, such as `64K`. |
| `--exclude-self` | Skip self-pair movements. |
| `--digits COUNT` | Digits after the decimal point in text output. |
| `--clock-probes COUNT` | Clock request/reply probes per arrival host. |
| `--json` | Print JSON. |
| `--api-key-env NAME` | API key environment variable. |

`file push` and `file pull` options:

| Argument or option | Meaning |
| --- | --- |
| `SOURCE` | Source file path on the source host. |
| `--remote NAME_OR_URL` | Required remote host name or URL. |
| `--entry NAME` | Entry or start host name. |
| `--dest PATH` | Destination path; defaults to the source basename. |
| `--mode copy|move` | Copy or move mode. |
| `--dry` | Only stat the source and report the planned destination. |
| `--overwrite` | Replace the destination if it already exists. |
| `--json` | Print JSON. |
| `--api-key-env NAME` | API key environment variable. |
