# Ping Pong

Ping Pong shows two paglets talking to each other. A `run` request makes
the paglet create a child from its own module, and the two bounce a
counter back and forth until it reaches the number of rounds asked for.
Then the parent answers `run` and tells the child to end.

Directory: [`cpp/examples/ping_pong/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/ping_pong)

## What it shows

- **Children.** `paglets::create_child` creates a new paglet, here from the
  same module, and returns an endpoint to it. An endpoint is a
  capability: a handle that lets one paglet send messages to another. A
  paglet reaches other paglets only through capabilities it created or was
  given (see [Capabilities](../writing-paglets.md#capabilities)).
- **One class, two roles.** Parent and child run the same code. The parent
  handles `run`, the child handles `ping` and `stop`; each only receives
  the messages of its role.
- **Requests with a continuation.** `request` sends `ping` and names the
  function that runs with the reply. The continuation is stored in the
  paglet's memory, so it would survive deactivation and moves.
- **Deferred replies.** The parent cannot answer `run` in its handler: the
  answer is known only many messages later. `defer_reply` returns a reply
  capability, and `reply_to` answers with it from a later handler.
- **Asynchronous replies.** The child answers with `reply_async`, which
  returns at once. A reply that cannot be delivered reaches
  `on_undelivered`.

## Why it is useful

Most paglets that do real work coordinate others: they start helpers,
wait for their answers, and reply to their own caller when everything is
done. Ping Pong shows that shape with nothing else around it. The handlers
never block: each one sends a message and returns, and the next step runs
when the reply arrives.

## How it works

The constructor registers the handlers of both roles. `run` refuses a
second run while one is pending (`bad_state`), creates the child, keeps
the reply capability of the request and starts the first round:

<!-- snippet: cpp/examples/ping_pong/ping_pong.cpp#roles -->
Source: [`cpp/examples/ping_pong/ping_pong.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/ping_pong/ping_pong.cpp)

```cpp
PingPong() {
    // Parent role.
    router().on<std::uint32_t>("run", [this](std::uint32_t rounds, paglets::Message& m) -> std::int32_t {
        if (pending_run_) return paglets::abi::bad_state;
        auto child = paglets::create_child(std::uint32_t{0});
        if (!child) return child.error();
        pong_ = *child;
        target_ = rounds;
        pending_run_ = m.defer_reply();
        bounce(0);
        return paglets::abi::handled;
    });
    // Child role. The reply is asynchronous: in a worker process it does
    // not wait for the host, and a failure would reach on_undelivered.
    router().on<std::uint32_t>("ping", [](std::uint32_t n, paglets::Message& m) { m.reply_async(n + 1); });
    router().on("stop", [](paglets::Message&) { (void)paglets::dispose(); });
}
```
<!-- /snippet -->

`bounce(n)` sends `ping` with `n`; the child answers `n + 1`, and the
continuation calls `bounce` with that value. When the target is reached,
the parent answers `run` with the final count and sends the child `stop`,
upon which the child disposes of itself:

<!-- snippet: cpp/examples/ping_pong/ping_pong.cpp#bounce -->
Source: [`cpp/examples/ping_pong/ping_pong.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/ping_pong/ping_pong.cpp)

```cpp
void bounce(std::uint32_t n) {
    if (n >= target_) {
        (void)paglets::reply_to(pending_run_, n);
        (void)pong_.send("stop");
        return;
    }
    auto sent = pong_.request("ping", n, [this](paglets::Message& reply) {
        std::uint32_t value = 0;
        if (!reply.ok() || !reply.decode(value)) {
            paglets::log(paglets::abi::LogLevel::error,
                         "ping failed: " + std::string(paglets::abi::error_name(reply.status())));
            (void)pending_run_.drop();
            return;
        }
        bounce(value);
    });
    if (!sent) (void)pending_run_.drop();
}
```
<!-- /snippet -->

If a `ping` fails, the parent logs the error and drops the reply
capability. Dropping it answers the `run` request with `gone`, so the
caller does not wait for its timeout.

The body of `run` and `ping` is a single unsigned integer. The paglet
needs no message header, because the SDK encodes plain integers and
strings itself.

## Running it

```bash
H=build/macos-arm64/host/paglets-host
$H run build/macos-arm64/guests/ping_pong.wasm --call run 25
```

`run` prints the reply `25` once the child has answered 25 pings. Then
`paglets-host` disposes of the parent; the child has already disposed of
itself.

## Test results

The test `runtime: ping-pong sample bounces between parent and child` in
`cpp/tests/test_runtime.cpp` creates the paglet and sends it `run` with 25
rounds. It checks that the reply has status 0 and is 25, and that after
the runtime is idle only one paglet is left: the child has disposed of
itself.

<!-- results: ping_pong -->
| Test | Mode | Outcome | Duration | Platform | Recorded | messages bounced between parent and child | paglets left afterwards (the child disposed itself) |
|---|---|---|---|---|---|---|---|
| runtime: ping-pong sample bounces between parent and child | in-process | passed |  | Darwin arm64, Release build | 2026-10-12 | 25 | 1 |
| runtime: ping-pong sample bounces between parent and child | worker processes | passed | 0.1 s | Darwin arm64, Release build | 2026-10-12 | 25 | 1 |
<!-- /results -->
