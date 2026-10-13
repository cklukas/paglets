# A life log of three hosts

The [Mesh Chronicle](mesh-chronicle.md) on this machine (mac) and two
Raspberry Pi class Linux hosts (pi, uconsole), with failures on schedule
(`cpp/tools/chronicle_live.sh`). This is the life log it delivered, as it is.

What happened to the hosts (seconds after the start):

- 91 s: the host of the main (pi) freezes (SIGSTOP): cut off, its main keeps its state
- 180 s: pi thaws (SIGCONT): two mains meet
- 271 s: pi crashes (SIGKILL)
- 360 s: pi restarts with its state
- 420 s: uconsole crashes (SIGKILL) and stays down for 90 s
- 510 s: uconsole restarts with its state
- 525 s: the sink's host (mac) shuts down normally
- 555 s: mac restarts with its state

## The life log

Written by the Mesh Chronicle, zombie group `9b9e3941061a0354a7d3af4487481e23`.

| | |
|---|---|
| Began | 2026-10-13 13:13:28 UTC |
| Ended | 2026-10-13 13:23:38 UTC |
| Lifetime | 10 min 09 s |
| Replicas | 3: mac, uconsole, pi |
| Samples of the mesh | 55, every 10 s |
| Takeovers | 2 |
| Time without a working main | 49 s (longest gap 34 s) |

### Timeline

| Time (UTC) | What happened |
|---|---|
| 2026-10-13 13:13:38 | The chronicle began on pi, with replicas on mac, uconsole, pi. |
| 2026-10-13 13:15:23 | **mac took over as main** (round_won, reign 2) after 34 s without a sample. |
| 2026-10-13 13:15:23 | **pi** disappeared (no fresh report seen from mac). |
| 2026-10-13 13:15:23 | mac lost its connection to pi (HTTP error: Failed to read connection). |
| 2026-10-13 13:15:23 | uconsole lost its connection to pi (HTTP error: Failed to read connection). |
| 2026-10-13 13:16:29 | **mac** disappeared (no fresh report seen from pi). |
| 2026-10-13 13:16:29 | **uconsole** disappeared (no fresh report seen from pi). |
| 2026-10-13 13:16:43 | **mac** is back (seen from mac). |
| 2026-10-13 13:16:43 | **uconsole** is back (seen from mac). |
| 2026-10-13 13:16:43 | **pi** is back (seen from mac). |
| 2026-10-13 13:16:43 | mac reaches pi again. |
| 2026-10-13 13:16:43 | uconsole reaches pi again. |
| 2026-10-13 13:18:03 | mac lost its connection to pi (opening a channel to https://192.168.86.215:7443: HTTP error: Could not establish connection). |
| 2026-10-13 13:18:03 | uconsole lost its connection to pi (opening a channel to https://192.168.86.215:7443: HTTP error: Could not establish connection). |
| 2026-10-13 13:18:13 | **pi** disappeared (no fresh report seen from mac). |
| 2026-10-13 13:19:33 | **pi** is back (seen from mac). |
| 2026-10-13 13:19:33 | mac reaches pi again. |
| 2026-10-13 13:19:33 | uconsole reaches pi again. |
| 2026-10-13 13:20:33 | mac lost its connection to uconsole (opening a channel to https://192.168.86.241:7443: HTTP error: Could not establish connection). |
| 2026-10-13 13:20:33 | pi lost its connection to uconsole (opening a channel to https://192.168.86.241:7443: HTTP error: Could not establish connection). |
| 2026-10-13 13:20:43 | **uconsole** disappeared (no fresh report seen from mac). |
| 2026-10-13 13:22:03 | **uconsole** is back (seen from mac). |
| 2026-10-13 13:22:03 | mac reaches uconsole again. |
| 2026-10-13 13:22:03 | pi reaches uconsole again. |
| 2026-10-13 13:22:38 | **uconsole took over as main** (takeover, reign 3) after 34 s without a sample. |
| 2026-10-13 13:22:38 | **mac** disappeared (no fresh report seen from uconsole). |
| 2026-10-13 13:22:38 | uconsole lost its connection to mac (opening a channel to https://192.168.86.216:7443: HTTP error: Could not establish connection). |
| 2026-10-13 13:22:38 | pi lost its connection to mac (opening a channel to https://192.168.86.216:7443: HTTP error: Could not establish connection). |
| 2026-10-13 13:22:48 | **mac** is back (seen from uconsole). |
| 2026-10-13 13:22:48 | uconsole reaches mac again. |
| 2026-10-13 13:22:48 | pi reaches mac again. |
| 2026-10-13 13:23:38 | The work is done on uconsole: 55 samples. |

### Connections

| Connection | Lost | Restored | For | Reason |
|---|---|---|---|---|
| mac – pi | 2026-10-13 13:15:23 | 2026-10-13 13:16:43 | 1 min 19 s | HTTP error: Failed to read connection |
| mac – pi | 2026-10-13 13:18:03 | 2026-10-13 13:19:33 | 1 min 30 s | opening a channel to https://192.168.86.215:7443: HTTP error: Could not establish connection |
| mac – pi | 2026-10-13 13:22:38 | 2026-10-13 13:22:48 | 10 s | opening a channel to https://192.168.86.216:7443: HTTP error: Could not establish connection |
| mac – uconsole | 2026-10-13 13:20:33 | 2026-10-13 13:22:03 | 1 min 30 s | opening a channel to https://192.168.86.241:7443: HTTP error: Could not establish connection |
| mac – uconsole | 2026-10-13 13:22:38 | 2026-10-13 13:22:48 | 10 s | opening a channel to https://192.168.86.216:7443: HTTP error: Could not establish connection |
| uconsole – pi | 2026-10-13 13:15:23 | 2026-10-13 13:16:43 | 1 min 19 s | HTTP error: Failed to read connection |
| uconsole – pi | 2026-10-13 13:18:03 | 2026-10-13 13:19:33 | 1 min 30 s | opening a channel to https://192.168.86.215:7443: HTTP error: Could not establish connection |
| uconsole – pi | 2026-10-13 13:20:33 | 2026-10-13 13:22:03 | 1 min 30 s | opening a channel to https://192.168.86.241:7443: HTTP error: Could not establish connection |

### Separations and partitions

| From | To | For | Sides | What it was |
|---|---|---|---|---|
| 2026-10-13 13:15:23 | 2026-10-13 13:16:43 | 1 min 19 s | {mac, uconsole} worked / {pi} worked | partition: both sides had a main |
| 2026-10-13 13:18:03 | 2026-10-13 13:19:33 | 1 min 30 s | {mac, uconsole} worked / {pi} silent | a host was gone or cut off |
| 2026-10-13 13:20:33 | 2026-10-13 13:22:03 | 1 min 30 s | {uconsole} silent / {mac, pi} worked | a host was gone or cut off |
| 2026-10-13 13:22:38 | 2026-10-13 13:22:48 | 10 s | {mac} silent / {uconsole, pi} worked | a host was gone or cut off |

1 partition(s) with a main on both sides. What both sides observed is merged into this log: the replicas that worked apart handed over their journals when they met again; the lineage that got furthest went on, the others forgot their work.
