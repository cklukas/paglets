# Hide and Seek

Hide and Seek is a game between two paglets. The seeker creates a hider,
which keeps moving from host to host. Round after round, the seeker finds
the hider wherever it is, pins it so that it cannot move on, checks that
it stays, and lets it go again. At the end, it reports where it caught the
hider each time.

Directory: [`cpp/examples/seek/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/seek)

## What it shows

- **Finding a paglet that keeps moving.** Every move of a paglet updates
  its location records. These live on hosts chosen by consistent hashing,
  so any host can answer where a paglet is. The `locator` system paglet
  looks them up (see
  [Location and pins](../hosts-and-meshes.md#location-and-pins)).
- **Pins.** `locate_and_pin` finds the paglet and pins it on its host in
  one step, so the answer is still true when the seeker acts on it. While
  the pin lasts, the host refuses the paglet's moves. A pin has a duration
  and is released explicitly; pinning needs a policy rule for service
  `locator`, operation `pin`.
- **`with_pinned`.** The patterns library pins a paglet, runs some work
  with its location, and releases the pin when the work says it is done
  (see [The patterns library](../writing-paglets.md#the-patterns-library)).
- **Messages that follow a moving paglet.** The seeker keeps one endpoint
  to the hider, and its requests reach the hider on whatever host it is
  at the time.

## Why it is useful

Paglets that move on their own, such as couriers, scouts or compute
workers that follow redirects, are hard to pin down from outside: by the
time an answer arrives, the paglet may be elsewhere. The game shows the
tools for this under continuous movement. The same steps let an owner or
another paglet hold a paglet in place while it inspects it, sends it work
that must happen on that host, or reads its results, and then let it
continue.

## How it works

The paglet starts as the seeker with a `start` message. It creates a child
of the same module, the hider, with the hop interval as its argument. The
hider sets a timer, and on each `hop` it moves to another host with the
transfer ticket `any`. After each arrival, it counts the move and sets the
timer again. While it is pinned, `dispatch` fails at once, and the hider
simply tries again after the next interval:

<!-- snippet: cpp/examples/seek/seek.cpp#hider -->
Source: [`cpp/examples/seek/seek.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/seek/seek.cpp)

```cpp
    void on_created(paglets::Started& s) override {
        // Created by a seeker: hide (args: the hop interval).
        if (!s.decode_args(hop_ms_) || hop_ms_ <= 0) return;
        hiding_ = true;
        (void)paglets::after_raw(hop_ms_, "hop", {});
    }

    void on_arrived(const paglets::abi::ArrivedEvent&) override {
        if (!hiding_) return;
        ++moves_;
        (void)paglets::after_raw(hop_ms_, "hop", {});
    }

    void on_move_failed(const paglets::abi::MoveFailed&) override {
        if (hiding_) (void)paglets::after_raw(hop_ms_, "hop", {});  // pinned, or nowhere to go: later
    }

private:
    // -- the hider ----------------------------------------------------------------------

    void hop() {
        if (!hiding_) return;
        // While pinned, dispatch says so at once: try again later.
        if (!paglets::dispatch("any")) (void)paglets::after_raw(hop_ms_, "hop", {});
    }
```
<!-- /snippet -->

The seeker plays `rounds` rounds, `pause_ms` apart. A round starts only
once the hider has moved since the last catch, however slow the hosts are,
so every catch finds it after a new move. The seeker then pins the hider
for at most 10 s with `with_pinned`, and records the host the locator
names and the number of moves the locator knows. While the pin holds, it
asks the hider how often it has moved (the hider answers from where it is
pinned), and then releases the pin:

<!-- snippet: cpp/examples/seek/seek.cpp#seek -->
Source: [`cpp/examples/seek/seek.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/seek/seek.cpp)

```cpp
// The next round starts once the hider has moved since the last catch,
// however slow the hosts are.
void round() {
    if (static_cast<std::int64_t>(score_.catches.size()) >= game_.rounds) return finish();
    hider_moves([this](std::int64_t n) {
        if (!score_.catches.empty() && n >= 0 && n <= caught_at_moves_) {
            (void)paglets::after_raw(game_.pause_ms, "next-round", {});
            return;
        }
        seek();
    });
}

void seek() {
    pt::with_pinned(hider_, 10'000, "hide and seek",
                    [this](paglets::Result<pt::Location> where, std::function<void()> done) {
                        Catch c;
                        if (where) {
                            c.host_name = where->host_name;
                            c.moves = where->moves;
                        } else {
                            c.error = std::string(paglets::abi::error_name(where.error()));
                        }
                        score_.catches.push_back(std::move(c));
                        // Pinned, it stays where it is while it counts.
                        hider_moves([this, done](std::int64_t n) {
                            caught_at_moves_ = n;
                            done();  // let it go
                            (void)paglets::after_raw(game_.pause_ms, "next-round", {});
                        });
                    });
}
```
<!-- /snippet -->

After the last round, the seeker asks the hider for its count of moves
once more, tells it to `stop`, and sets its state to `done`. The hider
stops moving and stays on the host where it is.

The messages and the score:

<!-- snippet: cpp/examples/seek/seek_msgs.hpp -->
Source: [`cpp/examples/seek/seek_msgs.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/seek/seek_msgs.hpp)

```cpp
// Messages of the Hide and Seek demo (planning/cpp-demo-paglets.md, demo 15).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace seek_msgs {

// `start`: the seeker creates a hider that keeps moving to other hosts
// every `hop_ms`; then, `rounds` times, it finds the hider, pins it,
// checks it stays, and lets it go.
struct Game {
    std::int64_t rounds = 5;
    std::int64_t hop_ms = 30;
    std::int64_t pause_ms = 300;  // between rounds: the hider moves on
};

struct Catch {
    std::string host_name;   // where the locator found it
    std::int64_t moves = 0;  // how often it had moved by then
    std::string error;
};

struct Score {
    std::string state;  // idle, seeking, done
    std::vector<Catch> catches;
    std::int64_t hider_moves = 0;  // as the hider counted, at the end
};

}  // namespace seek_msgs
```
<!-- /snippet -->

## Running it on a live mesh

The mesh needs at least two hosts, and a policy rule that lets the
owner's paglets pin. Add the rule to the admin's ledger and push it to a
host, which spreads it by gossip (see [ledger](../cli-reference.md#ledger)
and [Hosts and meshes](../hosts-and-meshes.md#policy)):

```bash
H=build/macos-arm64/host/paglets-host
$H ledger rule --ledger ledger --admin alice.key --name pins --decision allow \
    --service locator --op pin --max-duration 600000
$H remote push --connect https://lab-1:7443 --key alice.key --ledger ledger
```

Then launch the seeker and start the game:

```bash
R="--connect https://lab-1:7443 --key olga.key --ledger ledger"
ID=$($H remote launch $R build/macos-arm64/guests/seek.wasm)
$H remote call $R $ID start '{"rounds": 5, "hop_ms": 1000, "pause_ms": 3000}'
$H remote call $R $ID score
```

`score` answers with `state` `seeking` while the game runs, then `done`.
Each catch names the host where the hider was found and how often it had
moved by then, or an `error` if it could not be pinned. `hider_moves` is
the hider's own count at the end. Between rounds, `remote locate` and
`remote pins` show the hider and its pins from any host.

## Test results

The test plays four rounds on a mesh of three hosts inside one test
process, with a hop every 30 ms and 300 ms between rounds, and a policy
rule that allows pins
(`example: Hide and Seek ...` in `cpp/tests/test_examples.cpp`). It checks
that:

- the game reaches `done` with four catches;
- every catch names a host and has no error;
- each catch after the first found the hider after more moves than the one
  before: the hider was released and moved on between rounds;
- the hider moved at least three times in all.

<!-- results: seek -->
| Test | Mode | Outcome | Duration | Platform | Recorded | catches | moves of the hider |
|---|---|---|---|---|---|---|---|
| example: Hide and Seek finds and pins a paglet that keeps moving | in-process | passed | 1.7 s | Darwin arm64, Release build | 2026-10-12 | 4 | 35 |
| example: Hide and Seek finds and pins a paglet that keeps moving | worker processes | passed | 1.8 s | Darwin arm64, Release build | 2026-10-12 | 4 | 30 |
<!-- /results -->
