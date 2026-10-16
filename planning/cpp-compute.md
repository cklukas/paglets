# paglets/cpp: mesh information and compute services (WP16)

Status: implemented. Companion to the plan (WP16). The Python edition's
`mesh-info` and `compute-slots` services are the model; there is no
central scheduler or job queue: every host decides admission for itself
and tells the others what it has free. Code: `cpp/host/src/node/compute.cpp`,
contracts in `cpp/common/include/paglets/services/mesh_info.hpp` and
`compute_slots.hpp`, the example in `cpp/examples/pi/`; tests in
`cpp/tests/test_compute.cpp` and `host_mesh_discovery`.

## 1. mesh-info

Every host samples itself every 2 s (platform layer: CPUs, load average per
CPU, CPU use, memory, free space where it keeps state, paglets, compute
slots) and sends its **snapshot** (frame `mi-sync`) to every live host. A
host accepts a snapshot only from the host it describes (the channel proved
the sender); a newer one replaces an older one; snapshots older than 20 s
are not fresh, and after a minute they are dropped. Every host can thus
answer for the whole mesh.

| Operation | Request | Reply |
|---|---|---|
| `snapshot` | | this host |
| `landscape` | `max_age_ms` | every host with a fresh snapshot, this one first |
| `select` | `limit`, `max_load_per_cpu`, `min_memory_available`, `min_slots_free`, `labels`, `include_self`, `max_age_ms` | the hosts that fit, the best first (score: load per CPU, CPU use, memory use, slot use, queue); with none, the least loaded hosts and `fallback` |

Every paglet gets an endpoint to `mesh-info`.

## 2. compute-slots

A host has **slots** (default: its CPUs; `paglets-host serve --slots N`).
Job paglets ask the compute-slots of the host they are on:

| Operation | Request | Reply |
|---|---|---|
| `request_slot` | `job`, `cores`, `estimated_ms` | a Decision: `run_now` with a lease and `expires_ms`, `queued` with the position, `redirect` with a host, or `rejected` |
| `release_slot` | `lease` | `released` |
| `status` | | this host's slots, leases, queue, and the peers' slots as mesh-info last heard |
| `candidates` | `cores`, `limit`, `include_self` | hosts with the most free slots |

- **Admission**: with no queue and enough free slots the paglet runs at
  once. Otherwise, a host with free slots now (fresh snapshot, no queue)
  gets it (`redirect`); otherwise it is queued. More cores than the host has
  are `rejected` (or redirected to a host that has them). A paglet that
  asks again with the same job and core count keeps its lease and receives
  the actual reserved core count. Changing either is `rejected` with no new
  allocation; the original lease remains valid. Release it before asking
  for different work or a different core count.
- **Grants**: the lease queue is FIFO. Its head and the native job queue's
  head share priority by monotonic arrival time (native jobs win a tie).
  Later native jobs or direct reservations can use only the surplus beyond
  an older head's required cores. Running work is not preempted; progress
  depends on existing allocations ending. Repeating an unchanged queued
  request retains its position and original arrival time. A grant reaches the waiting paglet as
  the message `compute.granted` (a Decision with the lease), which activates
  it if it deactivated. If the message cannot be delivered (the paglet is
  gone), the lease is taken back.
- **Capacity changes**: lowering the total rejects queued leases that can
  no longer fit with `compute.rejected` (a Decision) and queued native jobs
  with `quota`. Existing allocations continue; new work waits until enough
  capacity is free. Departed waiters and cancelled native jobs lose their
  admission priority immediately when removed.
- **Spillover**: waiters behind the first (the first stays: it is next here)
  are offered to hosts with free slots, at most 4 per round and half the
  queue, with shadow reservations so one host is not flooded:
  `compute.redirect` (a Decision with the host). The scheduler never moves
  paglets; the paglet dispatches itself and asks there.
- **Expiry and renewal**: lifetime is the greater of `lease_base` (10 min
  by default) and twice a positive `estimated_ms`, within `lease_cap` (24 h
  by default and as a hard maximum). Nonpositive estimates use the base;
  multiplication saturates before overflow. The host normalizes timing
  settings to 1 ms through 24 h, with cap at least base. An unchanged queued
  repeat may update the estimate while retaining its position and age.
  Lifetime begins on grant, not enqueue. An unchanged allocated request
  renews without shortening its lifetime, but never past the continuous
  deadline fixed on the original grant. Timing changes apply to new grants
  and extension durations, leaving that original deadline fixed.
- **Leases end** on expiry, release, or when the paglet ends or leaves.
  Expiry frees accounting once and sends `compute.expired` (a rejected
  Decision with the old lease ID, cores and expiry). A late release returns
  false; a fresh request rejoins admission behind older waiting work.
  `Decision.expires_ms` and status `Lease.expires_ms` are Unix milliseconds
  for diagnostics; internal deadlines use a monotonic clock. Status retains
  the original `granted_ms`; `remote slots` prints both timestamps.
- **Scope**: leases are cooperative accounting, not CPU preemption. Native
  jobs retain their enforced wall-time limit. Leases and queues are not
  persisted: paglets request anew after restart or arrival elsewhere.

Every paglet gets an endpoint to `compute-slots`.

## 3. Images make jobs resilient

Paglets are memory images, so compute jobs need no special support to be
checkpointed and moved: the runtime checkpoints them, a job that moves
keeps its state, and a host that restarts resumes its paglets. A job that
spreads work as clones (section 5) holds the state that matters (which
chunks are done) in one paglet; chunks whose worker does not answer in time
go out again to other hosts, so the loss of a host costs only the chunks in
flight there. Waiting work moves to hosts with free slots (spillover), which
rebalances a chunked job while it runs. Dedicated workers (hosts or slots
reserved for one job) are left for later.

## 4. Command line

`paglets-host serve --slots N`; `paglets-host remote landscape` (every
host: CPUs, load, memory, slots, queue, paglets) and `paglets-host remote
slots` (leases, queue and peers of one host), as CLI session requests
`landscape` and `slots`.

## 5. The pi example

`cpp/examples/pi/` computes hexadecimal digits of pi with the
Bailey-Borwein-Plouffe formula, which yields digits at any position, so
every chunk is independent:

1. `start` (`digits`, `chunk`, `max_in_flight`, `chunk_timeout_ms`,
   `work_ms`): the job paglet cuts the work into chunks, asks mesh-info
   `select` for hosts with free slots, and sends a **clone** of itself with
   a chunk to each (`clone` with a destination); the clone gets the job's
   endpoint.
2. A worker asks its host's compute-slots for a slot; it runs at once, or
   waits for `compute.granted`, or follows `compute.redirect`. It computes
   its digits, sends them to the job (`result`), releases its lease and
   disposes itself.
3. The job keeps the chunks in order; `status` returns the digits so far,
   the chunks done and in flight, how many were sent again and the hosts
   that computed them. Every 500 ms it sends chunks that were not answered
   within `chunk_timeout_ms` to other hosts.

## 6. Exit

The exit of WP16: the pi compute example schedules across three hosts and
survives the loss of one host mid-run.

| Test | Covers |
|---|---|
| `test_compute.cpp`: mesh-info | every host knows every snapshot; `landscape`, `select` (free slots, fallback) through a paglet; a host that goes away drops out |
| compute-slots | run now, asking again keeps the lease, the queue and its positions, rejected, a redirect to a host whose slot freed up while the next waiter stays, a grant on release, a lease ending with its paglet |
| pi (exit) | three hosts over HTTPS with two slots each; 1200 digits in 40 chunks spread over all three; one host is shut down mid-run; its chunks go out again and the result is pi's digits |
| `host_mesh_discovery` | `remote landscape` lists the four hosts, `remote slots` answers |
