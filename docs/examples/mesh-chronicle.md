# Mesh Chronicle

The Mesh Chronicle is a zombie paglet: it keeps running as long as one
host of its mesh is alive. It writes the life log of its mesh: which hosts
joined the mesh, which disappeared and came back, which connections between hosts were lost and
restored, when the mesh was split into partitions, and how the chronicle
itself survived, with every takeover. When its lifetime is over, it
delivers the life log as Markdown to a sink and ends on every host.

Directory: [`cpp/examples/chronicle/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/chronicle)

## What it shows

- **A paglet that outlives its hosts.** The chronicle derives from
  `patterns::Zombie<Work>` ([Zombie paglets](../zombie-paglets.md)). It has a
  replica on every host; one replica, the main, samples the mesh, and the
  others take over from the latest checkpoint when the main disappears.
- **Partitions without lost observations.** When a host is cut off, its
  replica goes on alone while the others elect a new main. When they meet
  again, the lineage that got furthest goes on and the other forgets its
  work. The observations of both sides are kept, because journals are
  merged ([The zombie protocol](../zombie-protocol.md#journals-observations-survive-every-partition)).
- **Restarts.** Replicas come back from their memory images after a host
  restarts (ABI v1.2), also after the whole mesh was down.
- **Exactly one result.** The life log goes to the `zombies` system paglet
  of the sink host, which accepts the first result of the group only.
- **Connectivity from mesh-info.** Every host's `mesh-info` snapshot lists
  its connections to the other hosts: up or down, since when, and why
  frames fail.

## How it works

The owner launches the chronicle and sends `start` with the sink, the
lifetime and the interval between samples. The chronicle starts the group:
a replica on every host with a fresh `mesh-info` snapshot.

Each step of the main asks `mesh-info` for the landscape and compares it
with what the journal says:

<!-- snippet: cpp/examples/chronicle/chronicle.cpp#step -->
Source: [`cpp/examples/chronicle/chronicle.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/chronicle/chronicle.cpp)

```cpp
void step(Work& w, std::function<void(bool done)> finished) override {
    auto mi = service("mesh-info");
    if (!mi) return finished(false);
    services::mesh_info::Client client{*mi};
    services::mesh_info::LandscapeRequest q;
    q.max_age_ms = std::max<std::int64_t>(3 * w.sample_ms, 3000);
    auto sent = client.landscape(q, [this, &w, finished](Result<services::mesh_info::Landscape> r, Message&) {
        if (!r) return finished(false);
        sample(w, r->hosts);
        const bool done = finish_ || now_ms() - w.started_ms >= w.lifetime_ms;
        if (done) note(Event{now_ms(), "end", here_, {}, std::to_string(w.samples) + " samples"});
        finished(done);
    });
    if (!sent) finished(false);
}
```
<!-- /snippet -->

The view to compare with is derived from the merged journal, not from the
work of the current lineage. So when the lineages of a partition meet, the
winner sees what the other side recorded (a host gone, a connection lost)
and records the change back as soon as it happens:

<!-- snippet: cpp/examples/chronicle/chronicle.cpp#view -->
Source: [`cpp/examples/chronicle/chronicle.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/chronicle/chronicle.cpp)

```cpp
// What the merged journal says now: every lineage's observations,
// the latest per host and per link.
std::map<std::string, bool> host_up;
std::set<std::string> down;  // "observer>other"
for (const auto& m : w.members) host_up[m] = true;
for (const auto& e : journal_events()) {
    if (e.kind == "host_down" || e.kind == "host_up") host_up[e.host] = e.kind == "host_up";
    if (e.kind == "link_down") down.insert(e.host + ">" + e.other);
    if (e.kind == "link_up") down.erase(e.host + ">" + e.other);
}
std::set<std::string> fresh;
for (const auto& h : hosts) fresh.insert(h.host);
for (const auto& [h, was_up] : host_up) {
    if (fresh.contains(h) && !was_up) note(Event{now, "host_up", h, here_});
    if (!fresh.contains(h) && was_up) note(Event{now, "host_down", h, here_});
}
// Links as each fresh host reports them (hosts that are not fresh
// keep what they reported last).
for (const auto& h : hosts) {
    for (const auto& l : h.links) {
        const std::string key = h.host + ">" + l.host;
        if (!l.up && !down.contains(key)) note(Event{now, "link_down", h.host, l.host, l.failure});
        if (l.up && down.contains(key)) note(Event{now, "link_up", h.host, l.host});
    }
}
```
<!-- /snippet -->

A replica that becomes main notes its takeover, with the cause and the
time since the last sample of its lineage. When the lifetime is over, the
step reports the work done; the main builds the life log from the merged
journal and delivers it, and the group ends on every host.

The life log has four parts:

- a summary: lifetime, replicas, samples, takeovers, and the time without a
  working main;
- the timeline of every event, from every lineage, in time order;
- the connections that were lost, with when they were restored and why they
  failed;
- the separations: times when hosts could not see each other, with the
  sides. A side "worked" when a main there recorded something. If both
  sides worked, it was a partition with a main on each side; if one side
  stayed silent, a host was gone.

## Running it

On a mesh of hosts that serve (see [Hosts and meshes](../hosts-and-meshes.md)):

```bash
H=build/macos-arm64/host/paglets-host
R="--connect https://lab-1:7443 --key olga.key --ledger ledger"
ID=$($H remote launch $R build/macos-arm64/guests/chronicle.wasm)
$H remote call $R $ID start '{"sink": "host:lab-1", "lifetime_ms": 600000, "sample_ms": 10000}'
$H remote call $R $ID status
```

`start` answers with the group ID. `status` shows the replica's role, the
main it follows, and the samples so far. While it runs:

```bash
$H remote zombies list $R            # the groups on this host, and their state
$H remote zombies traces GROUP $R    # every host's protocol steps, in time order
```

After the end, the life log is at the sink, and the protocol steps can be
checked against the specification:

```bash
$H remote zombies result GROUP $R > life-log.md
$H remote zombies check GROUP $R
```

`finish` ends a chronicle early (sent to the replica that is the main).

## A life log from three hosts

`cpp/tools/chronicle_live.sh --late uconsole` runs the chronicle for 10
minutes on this Mac and two Linux hosts (a Raspberry Pi 4 and a uConsole,
both arm64). The uConsole joins the mesh a minute after the start and gets a
replica. While the chronicle runs, the script freezes the host of the main
for 90 seconds (cut off, its main keeps working alone), crashes hosts and
restarts them, and shuts the sink's host down for a while. The life log it delivered is in
[A life log of three hosts](mesh-chronicle-log.md).

<!-- results: live_chronicle -->
| Test | Mode | Outcome | Duration | Platform | Recorded | hosts with a replica | lifetime (min) | takeovers in the life log | partitions with a main on both sides | protocol steps checked against the specification | replicas left after the end |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Mesh Chronicle, 10 minutes with a host joining, freezes, crashes and restarts | LAN, worker processes | passed | 10 min 13 s | macOS arm64, 2 × Linux arm64, Release build | 2026-10-13 | 3 | 10 | 2 | 1 | 20 | 0 |
<!-- /results -->

## Test results

The test runs three hosts in one process and adds a fourth while the
chronicle runs, which gets a replica. It cuts the main's host off,
lets both sides elect and work, restarts the cut-off host, and waits for
the life log. The life log must show the new host, the takeovers, the host that
disappeared and came back, and the partition with a main on both sides.
The protocol steps that all hosts recorded must conform to the
specification. Its name is
`example: the Mesh Chronicle outlives a partition and a restart of its main's host`
in `cpp/tests/test_zombies.cpp`.

<!-- results: chronicle -->
| Test | Mode | Outcome | Duration | Platform | Recorded | hosts with a replica (one joined the mesh during the run) | takeovers recorded by the hosts | protocol steps checked against the specification | results accepted by the sink | size of the life log (bytes) |
|---|---|---|---|---|---|---|---|---|---|---|
| example: the Mesh Chronicle outlives a partition and a restart of its main's host | in-process | passed | 21.0 s | Darwin arm64, RelWithDebInfo build | 2026-10-13 | 4 | 4 | 13 | 1 | 2480 |
| example: the Mesh Chronicle outlives a partition and a restart of its main's host | worker processes | passed | 21.6 s | Darwin arm64, RelWithDebInfo build | 2026-10-13 | 4 | 4 | 13 | 1 | 2390 |
<!-- /results -->
