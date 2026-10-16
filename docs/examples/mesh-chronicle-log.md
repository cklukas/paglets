# A life log of three hosts

The [Mesh Chronicle](mesh-chronicle.md) on this machine (mac) and two
Raspberry Pi class Linux hosts (pi, uconsole), with failures on schedule
(`cpp/tools/chronicle_live.sh --late uconsole`). This is the life log it delivered, as it is.

> [!NOTE]
> The historical summary below labels 53 s as time without a working main.
> That value is the sum of takeover sample gaps beyond one nominal 10 s
> interval each; 38 s is the longest raw gap. Gaps from different lineages
> can overlap, and delayed sample callbacks contribute. These values do
> not measure exact main downtime. Current Chronicle output names the two
> quantities separately and explains their limits.

What happened to the hosts (seconds after the start):

- 60 s: uconsole joins the mesh (its host starts)
- 92 s: the host of the main (pi) freezes (SIGSTOP): cut off, its main keeps its state
- 180 s: pi thaws (SIGCONT): two mains meet
- 271 s: pi crashes (SIGKILL)
- 360 s: pi restarts with its state
- 420 s: uconsole crashes (SIGKILL) and stays down for 90 s
- 510 s: uconsole restarts with its state
- 525 s: the sink's host (mac) shuts down normally
- 555 s: mac restarts with its state

## The life log

Written by the Mesh Chronicle, zombie group `cd8f9f3d13f598c658b4bcda95c39257`.

| | |
|---|---|
| Began | 2026-10-13 17:14:34 UTC |
| Ended | 2026-10-13 17:24:44 UTC |
| Lifetime | 10 min 09 s |
| Replicas | 3: mac, pi, uconsole |
| Samples of the mesh | 55, every 10 s |
| Takeovers | 2 |
| Time without a working main | 53 s (longest gap 38 s) |

### Timeline

| Time (UTC) | What happened |
|---|---|
| 2026-10-13 17:14:44 | The chronicle began on pi, with replicas on mac, pi. |
| 2026-10-13 17:14:44 | pi lost its connection to uconsole (opening a channel to https://192.168.86.241:7443: HTTP error: Could not establish connection). |
| 2026-10-13 17:14:44 | mac lost its connection to uconsole (opening a channel to https://192.168.86.241:7443: HTTP error: Could not establish connection). |
| 2026-10-13 17:15:44 | pi reaches uconsole again. |
| 2026-10-13 17:15:44 | mac reaches uconsole again. |
| 2026-10-13 17:16:04 | **uconsole** joined the mesh and got a replica (seen from pi). |
| 2026-10-13 17:16:33 | **mac took over as main** (round_won, reign 2) after 38 s without a sample. |
| 2026-10-13 17:16:33 | **pi** disappeared (no fresh report seen from mac). |
| 2026-10-13 17:16:33 | mac lost its connection to pi (HTTP error: Failed to read connection). |
| 2026-10-13 17:16:33 | uconsole lost its connection to pi (HTTP error: Failed to read connection). |
| 2026-10-13 17:17:34 | **mac** disappeared (no fresh report seen from pi). |
| 2026-10-13 17:17:34 | **uconsole** disappeared (no fresh report seen from pi). |
| 2026-10-13 17:17:48 | **mac** is back (seen from mac). |
| 2026-10-13 17:17:48 | **pi** is back (seen from mac). |
| 2026-10-13 17:17:48 | **uconsole** is back (seen from mac). |
| 2026-10-13 17:17:48 | mac reaches pi again. |
| 2026-10-13 17:17:48 | uconsole reaches pi again. |
| 2026-10-13 17:19:08 | mac lost its connection to pi (opening a channel to https://192.168.86.215:7443: HTTP error: Could not establish connection). |
| 2026-10-13 17:19:08 | uconsole lost its connection to pi (opening a channel to https://192.168.86.215:7443: HTTP error: Could not establish connection). |
| 2026-10-13 17:19:18 | **pi** disappeared (no fresh report seen from mac). |
| 2026-10-13 17:20:38 | **pi** is back (seen from mac). |
| 2026-10-13 17:20:38 | mac reaches pi again. |
| 2026-10-13 17:20:38 | uconsole reaches pi again. |
| 2026-10-13 17:21:39 | mac lost its connection to uconsole (opening a channel to https://192.168.86.241:7443: HTTP error: Could not establish connection). |
| 2026-10-13 17:21:39 | pi lost its connection to uconsole (opening a channel to https://192.168.86.241:7443: HTTP error: Could not establish connection). |
| 2026-10-13 17:21:49 | **uconsole** disappeared (no fresh report seen from mac). |
| 2026-10-13 17:23:09 | **uconsole** is back (seen from mac). |
| 2026-10-13 17:23:09 | mac reaches uconsole again. |
| 2026-10-13 17:23:09 | pi reaches uconsole again. |
| 2026-10-13 17:23:44 | **pi took over as main** (takeover, reign 3) after 34 s without a sample. |
| 2026-10-13 17:23:44 | **mac** disappeared (no fresh report seen from pi). |
| 2026-10-13 17:23:44 | pi lost its connection to mac (opening a channel to https://192.168.86.216:7443: HTTP error: Could not establish connection). |
| 2026-10-13 17:23:44 | uconsole lost its connection to mac (opening a channel to https://192.168.86.216:7443: HTTP error: Could not establish connection). |
| 2026-10-13 17:23:54 | **mac** is back (seen from pi). |
| 2026-10-13 17:23:54 | pi reaches mac again. |
| 2026-10-13 17:23:54 | uconsole reaches mac again. |
| 2026-10-13 17:24:44 | The work is done on pi: 55 samples. |

### Connections

| Connection | Lost | Restored | For | Reason |
|---|---|---|---|---|
| mac – pi | 2026-10-13 17:16:33 | 2026-10-13 17:17:48 | 1 min 15 s | HTTP error: Failed to read connection |
| mac – pi | 2026-10-13 17:19:08 | 2026-10-13 17:20:38 | 1 min 30 s | opening a channel to https://192.168.86.215:7443: HTTP error: Could not establish connection |
| mac – pi | 2026-10-13 17:23:44 | 2026-10-13 17:23:54 | 10 s | opening a channel to https://192.168.86.216:7443: HTTP error: Could not establish connection |
| mac – uconsole | 2026-10-13 17:14:44 | 2026-10-13 17:15:44 | 1 min 00 s | opening a channel to https://192.168.86.241:7443: HTTP error: Could not establish connection |
| mac – uconsole | 2026-10-13 17:21:39 | 2026-10-13 17:23:09 | 1 min 30 s | opening a channel to https://192.168.86.241:7443: HTTP error: Could not establish connection |
| mac – uconsole | 2026-10-13 17:23:44 | 2026-10-13 17:23:54 | 10 s | opening a channel to https://192.168.86.216:7443: HTTP error: Could not establish connection |
| pi – uconsole | 2026-10-13 17:14:44 | 2026-10-13 17:15:44 | 1 min 00 s | opening a channel to https://192.168.86.241:7443: HTTP error: Could not establish connection |
| pi – uconsole | 2026-10-13 17:16:33 | 2026-10-13 17:17:48 | 1 min 15 s | HTTP error: Failed to read connection |
| pi – uconsole | 2026-10-13 17:19:08 | 2026-10-13 17:20:38 | 1 min 30 s | opening a channel to https://192.168.86.215:7443: HTTP error: Could not establish connection |
| pi – uconsole | 2026-10-13 17:21:39 | 2026-10-13 17:23:09 | 1 min 30 s | opening a channel to https://192.168.86.241:7443: HTTP error: Could not establish connection |

### Separations and partitions

| From | To | For | Sides | What it was |
|---|---|---|---|---|
| 2026-10-13 17:14:44 | 2026-10-13 17:15:44 | 1 min 00 s | {mac, pi} worked / {uconsole} silent | a host was gone or cut off |
| 2026-10-13 17:16:33 | 2026-10-13 17:17:48 | 1 min 15 s | {pi} worked / {mac, uconsole} worked | partition: both sides had a main |
| 2026-10-13 17:19:08 | 2026-10-13 17:20:38 | 1 min 30 s | {pi} silent / {mac, uconsole} worked | a host was gone or cut off |
| 2026-10-13 17:21:39 | 2026-10-13 17:23:09 | 1 min 30 s | {mac, pi} worked / {uconsole} silent | a host was gone or cut off |
| 2026-10-13 17:23:44 | 2026-10-13 17:23:54 | 10 s | {mac} silent / {pi, uconsole} worked | a host was gone or cut off |

1 partition(s) with a main on both sides. What both sides observed is merged into this log: the replicas that worked apart handed over their journals when they met again; the lineage that got furthest went on, the others forgot their work.
