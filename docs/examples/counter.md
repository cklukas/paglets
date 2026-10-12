# Counter

The Counter keeps a value and a history of the steps that changed it. Its
state is ordinary C++ objects: an integer, a `std::vector` of structs, and
buffers on the heap. A memory image saves and restores all of it, without
a line of serialization code. The Counter also defines its messages in a
header of their own, from which the build generates the encoders.

Directory: [`cpp/examples/counter/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/counter)

## What it shows

- **Memory images.** A paglet is saved, moved and resumed as an image of
  its WebAssembly memory, its linear memory. Everything the paglet owns
  lives there, on the heap or in static storage, so the image carries it
  as it is: the `std::vector` of the history, the strings in it, and the
  heap buffers of `bloat`.
- **Typed messages from a schema header.** The message types are plain
  structs in `messages.hpp`. The guest compiler has no reflection, so
  `schema_gen` generates their MessagePack encoders and a schema
  description for the guest. The host encodes the same structs through
  C++26 reflection (see
  [Messages and schemas](../writing-paglets.md#messages-and-schemas)).
- **Every supported field type.** `echo` returns its body unchanged. Its
  type `Echo` has booleans, signed and unsigned integers of several
  widths, `float` and `double`, strings, byte vectors, vectors of structs,
  `std::optional` values and an enum.
- **The paglet's size.** `status` reports the size of the paglet's memory
  in WebAssembly pages of 64 KB, and `bloat` allocates memory on purpose,
  for experiments with the size of images.

## Why it is useful

The Counter is the paglet the tests and the command-line examples use
whenever they need state that must survive: across a host restart, a
copy or a move between hosts. Its state has the shapes real paglets have,
such as containers of structs and optional fields, and a reply tells at
once whether the state came through. It is also the example to start from
for a paglet with a message header of its own.

## How it works

The constructor registers the handlers. `increment` adds `by` to the value
or multiplies the value by it, and keeps the step in the history:

<!-- snippet: cpp/examples/counter/counter.cpp#handlers -->
Source: [`cpp/examples/counter/counter.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/counter/counter.cpp)

```cpp
Counter() {
    router().on<Increment>("increment", [this](const Increment& inc, paglets::Message& m) {
        value_ = inc.mode == Mode::add ? value_ + inc.by : value_ * inc.by;
        history_.push_back(inc);
        m.reply(status());
    });
    router().on("status", [this](paglets::Message& m) { m.reply(status()); });
    router().on<Echo>("echo", [](const Echo& echo, paglets::Message& m) { m.reply(echo); });
    router().on<Bloat>("bloat", [this](const Bloat& bloat, paglets::Message& m) {
        ballast_.emplace_back(std::size_t{bloat.kilobytes} * 1024, bloat.fill);
        m.reply(status());
    });
    router().on("schema", [](paglets::Message& m) { m.reply(std::string(paglets_schema_json)); });
    router().on<std::string>("log", [this](const std::string& text, paglets::Message& m) {
        paglets::log(text);
        m.reply(status());
    });
}
```
<!-- /snippet -->

`schema` returns `paglets_schema_json`, the description of the message
types that the build generated from the header. `log` writes a text to the
host log.

The reply to `increment`, `status`, `bloat` and `log` is a `Status`. It
holds the value, the number of steps, the notes of the last three steps,
the mean step size (absent before the first step) and the number of
memory pages. The state behind it is three members:

<!-- snippet: cpp/examples/counter/counter.cpp#status -->
Source: [`cpp/examples/counter/counter.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/counter/counter.cpp)

```cpp
Status status() const {
    Status s;
    s.value = value_;
    s.history_size = static_cast<std::uint32_t>(history_.size());
    const std::size_t first = history_.size() > 3 ? history_.size() - 3 : 0;
    for (std::size_t i = first; i < history_.size(); ++i) {
        s.recent_notes.push_back(history_[i].note);
    }
    if (!history_.empty()) {
        double sum = 0;
        for (const auto& h : history_) sum += static_cast<double>(h.by);
        s.average_step = sum / static_cast<double>(history_.size());
    }
    s.memory_pages = static_cast<std::uint32_t>(__builtin_wasm_memory_size(0));
    return s;
}

std::int64_t value_ = 0;
std::vector<Increment> history_;
std::vector<std::vector<std::uint8_t>> ballast_;
```
<!-- /snippet -->

The messages:

<!-- snippet: cpp/examples/counter/messages.hpp -->
Source: [`cpp/examples/counter/messages.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/counter/messages.hpp)

```cpp
// Message types of the counter example paglet. Plain structs: the host encodes
// them through C++26 reflection, the guest through code generated from this
// header by schema_gen.

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace counter_msgs {

enum class Mode { add, multiply };

struct Increment {
    std::int64_t by = 1;
    std::string note;
    Mode mode = Mode::add;
};

struct Status {
    std::int64_t value = 0;
    std::uint32_t history_size = 0;
    std::vector<std::string> recent_notes;
    std::optional<double> average_step;
    std::uint32_t memory_pages = 0;
};

// Exercises every supported field type; the guest echoes it unchanged.
struct Echo {
    bool flag = false;
    std::int8_t tiny = 0;
    std::int32_t medium = 0;
    std::uint64_t big = 0;
    float ratio = 0.0f;
    double precise = 0.0;
    std::string text;
    std::vector<std::uint8_t> blob;
    std::vector<Increment> items;
    std::optional<std::string> label;
    std::optional<Status> nested;
    Mode mode = Mode::add;
};

// Allocates and keeps the given amount of memory (image size experiments).
struct Bloat {
    std::uint32_t kilobytes = 0;
    std::uint8_t fill = 0;
};

}  // namespace counter_msgs
```
<!-- /snippet -->

The module names the header and its namespace, so the build runs
`schema_gen` for it:

```cmake
paglets_add_module(counter
    SOURCES counter/counter.cpp
    SCHEMA_HEADER counter/messages.hpp
    SCHEMA_NAMESPACE counter_msgs)
```

## Running it

Bodies on the command line are JSON; enums are written as their names:

```bash
H=build/macos-arm64/host/paglets-host
$H run build/macos-arm64/guests/counter.wasm \
    --call increment '{"by": 5, "note": "first"}' \
    --call increment '{"by": 3, "note": "triple", "mode": "multiply"}' \
    --call status
```

The value is 5 after the first call and 15 after the second. `status`
answers with `history_size` 2, the notes `first` and `triple`, and
`average_step` 4.

With a state directory, the paglet outlives the host process. `--keep`
keeps it after the calls, and `call` resumes it from its last memory image
in a new process (see
[Running paglets locally](../writing-paglets.md#running-paglets-locally)):

```bash
$H run build/macos-arm64/guests/counter.wasm --state-dir state --keep \
    --call increment '{"by": 5, "note": "a"}' --call increment '{"by": 2, "note": "b"}'
$H list --state-dir state
$H call --state-dir state all increment '{"by": 1, "note": "c"}'
```

The last call answers with the value 8: the 7 of the first process, plus
1.

## Test results

The test `snapshot: counter state survives capture, serialize and restore`
in `cpp/tests/test_snapshot.cpp` works with the WebAssembly engine
directly, without a host:

- it sends ten `increment` requests with the steps 1 to 10;
- it captures the memory image, serializes it to bytes, reads it back, and
  restores it into a new instance of the module. Besides the memory, the
  image holds one global, the stack pointer;
- the restored paglet answers `status` with the value 55;
- both copies then go on independently: the restored one reaches 155 with
  a step of 100, the original 56 with a step of 1.

Two command-line tests in `cpp/tests/CMakeLists.txt` run the second
example above: `host_state_save` runs the Counter with `--state-dir` and
`--keep` and increments it by 5 and 2, and `host_state_resume` increments
it by 1 in a new process and expects `"value":8`. `host_serve_move`
launches the Counter on one of two hosts that serve over HTTPS, moves it
to the other host and back with `remote dispatch`, and increments it on
each host; `host_mesh_discovery` uses it as well.

<!-- results: counter -->
| Test | Mode | Outcome | Duration | Platform | Recorded | sum after ten increments, restored from its memory image | memory image size, serialized (bytes) |
|---|---|---|---|---|---|---|---|
| snapshot: counter state survives capture, serialize and restore | in-process | passed |  | Darwin arm64, Release build | 2026-10-12 | 55 | 131212 |
| snapshot: counter state survives capture, serialize and restore | worker processes | passed | 0.1 s | Darwin arm64, Release build | 2026-10-12 | 55 | 131212 |
<!-- /results -->
