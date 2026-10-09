# paglets/cpp: benchmarks (WP20)

Status: the first set is implemented; numbers below are from one machine
and change with it. Code: `cpp/tests/test_bench.cpp` (`bench:` tests of
the unit tests).

## 1. Running them

In CI the benchmarks run briefly, as smoke tests, in both modes of the
suite (in-process and with worker processes). At full size:

```bash
PAGLETS_BENCH=1 build/linux-gcc16/tests/paglets_tests build/linux-gcc16/guests "bench:"
PAGLETS_BENCH=1 PAGLETS_TEST_WORKER=build/linux-gcc16/host/paglets-worker \
    build/linux-gcc16/tests/paglets_tests build/linux-gcc16/guests "bench:"
```

`PAGLETS_BENCH_JSON=FILE` appends one JSON object per benchmark to `FILE`.

## 2. What they measure

| Benchmark | Measures |
|---|---|
| `request-reply` | request and reply between the host and an active paglet (the conformance guest's `echo`), one at a time: rate, median and 90th percentile |
| `activation` | a message to an inactive paglet: its image is loaded, the instance placed, `activated` delivered and the message handled |
| `memory-per-paglet` | growth of the host process's resident memory per counter paglet (in worker mode only the host's share: the instances live in the workers) |
| `move` | a paglet with 4 MB of unchanging memory moving between two hosts (in-memory transport, so no network): latency, pages on the first move, pages on later moves (page reuse), compressed bytes |

## 3. First numbers

Linux x86_64, 4 CPUs, GCC 16, WAMR fast interpreter, `PAGLETS_BENCH=1`:

| Benchmark | In-process | Worker processes |
|---|---|---|
| request-reply | 15 500 per second, median 0.06 ms | 15 200 per second, median 0.05 ms |
| activation | median 0.64 ms | median 5.9 ms |
| memory per paglet | 134 KB | 46 KB (host share) |
| move (median) | 31 ms | 93 ms |
| pages per move | 66 on the first, 2.2 on later moves | the same |

Page reuse does what it should: after the first move only the pages that
changed travel (a 4 MB ballast is sent once). Activation with worker
processes pays for the image transfer to the worker.

## 4. Still to do

Message throughput between paglets (also across hosts over HTTPS), and
moves over the HTTPS transport between machines (the rpi4 and the CI
runners of each platform).
