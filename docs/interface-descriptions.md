# Interface descriptions

An interface description says which messages a paglet understands, which
types their payloads and replies have, and which arguments the paglet is
started with. It travels inside the module, in a Wasm custom section named
`paglets.interface`.

From it:

- [`paglets-web`](web-ui.md) builds forms;
- `paglets-host remote describe` prints it;
- `paglets-host module inspect` shows it for a file.

The section is part of the module's bytes, so its hash and the ledger's
module signatures cover it. Hosts read it without running the paglet.

## Format

<!-- snippet: cpp/examples/counter/interface.json -->
Source: [`cpp/examples/counter/interface.json`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/counter/interface.json)

```json
{
  "interface": 1,
  "name": "counter",
  "summary": "A counter whose state is plain C++ objects; memory images move it",
  "launch": {"type": "Start", "example": {"value": 0}},
  "messages": [
    {"name": "increment", "kind": "both", "summary": "Adds to or multiplies the value; replies with the status",
     "request": "Increment", "reply": "Status", "example": {"by": 1, "note": "from the web", "mode": "add"}},
    {"name": "status", "kind": "call", "summary": "The value and the latest notes", "request": "none", "reply": "Status"},
    {"name": "echo", "kind": "call", "summary": "Returns the record unchanged (every field type)", "request": "Echo", "reply": "Echo",
     "example": {"flag": true, "tiny": -7, "medium": 300, "big": 1234567890123,
                 "ratio": 1.5, "precise": 2.25, "text": "hello", "blob": {"$bytes": "00ff"},
                 "items": [{"by": 1, "note": "example", "mode": "add"}],
                 "label": "sample", "nested": null, "mode": "add"}},
    {"name": "bloat", "kind": "call", "summary": "Keeps the given amount of memory (image size experiments)",
     "request": "Bloat", "reply": "Status"},
    {"name": "log", "kind": "both", "summary": "Writes a line to the host's log; the request is a bare string", "request": "string", "reply": "Status"},
    {"name": "schema", "kind": "call", "summary": "The generated message schema as JSON text", "request": "none", "reply": "string"}
  ],
  "types": [
    {"name": "Start", "kind": "record", "fields": [
      {"name": "value", "type": "i64", "doc": "The value to begin with", "default": 0}]},
    {"name": "Increment", "kind": "record", "fields": [
      {"name": "by", "type": "i64", "doc": "Amount (or factor)", "default": 1},
      {"name": "note", "type": "string", "doc": "Kept in the history"},
      {"name": "mode", "type": "Mode", "default": "add"}]},
    {"name": "Bloat", "kind": "record", "fields": [
      {"name": "kilobytes", "type": "u32", "doc": "Memory to keep, in KB", "default": 1024},
      {"name": "fill", "type": "u8", "doc": "Byte value written into it"}]}
  ]
}
```
<!-- /snippet -->

The schema header adds `Mode`, `Status` and `Echo` at build time. Types
already written in the description take precedence. The source above
contains the handwritten launch/request records; the built module carries
the merged description.


| Member | Meaning |
|---|---|
| `interface` | the format version, `1` |
| `name`, `summary` | what the module is, in a few words |
| `launch.type` | the type of the start arguments (`--args`), `none` if there are none; `launch.example` fills the form |
| `messages[].name` | the message name the paglet's router handles; names starting with `paglets.` are reserved |
| `messages[].kind` | `call` (a request with a reply), `send` (one-way) or `both`; default `call`. Sending a `both` message runs its handler without a reply capability; its reply is not delivered |
| `messages[].request`, `messages[].reply` | payload types; omitted fields default to `none` for no payload |
| `messages[].summary` | one line about what the message does |
| `messages[].danger` | `true`: `paglets-web` asks for confirmation (for example before a message that ends work); the CLI labels it `web confirmation` and executes calls and sends directly |
| `messages[].example` | a payload that fills the form |
| `types` | records (`fields`: `name`, `type`, optional `doc` and `default`) and enums (`values`) |

Types are the names of the schema generator:

| Type | Values |
|---|---|
| `bool` | true or false |
| `i8`, `i16`, `i32`, `i64`, `u8`, `u16`, `u32`, `u64` | integers |
| `f32`, `f64` | floating-point numbers |
| `string` | text |
| `bytes` | binary strings (in JSON: `{"$bytes": "00ff"}`) |
| `list<T>`, `optional<T>` | lists, and values that may be missing (`null`) |
| a record or enum name | a type of `types` |
| `any` | any JSON value (the UI shows a JSON field) |

Records encode as MessagePack maps with their field names, enums as their
value's name. These are the encodings of the generated codecs, so a
description matches the C++ types of a schema header.

## Adding one to a module

Write the description next to the paglet's sources and name it in the
build:

```cmake
paglets_add_module(counter
    SOURCES counter/counter.cpp
    SCHEMA_HEADER counter/messages.hpp
    SCHEMA_NAMESPACE counter_msgs
    INTERFACE counter/interface.json)
```

After linking, the build runs the interface tool (`paglets-interface`). It
adds the records and enums of the schema header to `types` (a type the
description defines itself supplies `doc` and `default`). A repeated type
must have the same field names/types or enum values as the schema; a
renamed field or changed wire type fails the build. The tool then checks
the description and writes it into the module. An unknown type, repeated
message or invalid `kind` fails the build with the reason. Launch/message
examples and field defaults are checked recursively, including numeric
ranges, enum values, byte encoding and unknown record fields. Omitted
record fields use the guest's defaults.
Nested type expressions and example/default values are limited to 128
levels, so hostile metadata cannot exhaust the host's call stack.

A module whose schema namespace has
[service contracts](sdk-reference.md) needs no file: it gets a description
with one `call` message per operation.

Projects outside the repository that use the installed package
(`find_package(paglets)`) have the same option. The package compiles the
tool from its installed source, like the schema generator.

The examples hello, ping_pong, counter, pi, inventory and chronicle carry
descriptions. The counter's is in `cpp/examples/counter/interface.json`.

## Reading one

With `H=build/linux-gcc16/host/paglets-host`:

```bash
$H module inspect build/linux-gcc16/guests/counter.wasm         # the end of the output
$H module inspect build/linux-gcc16/guests/counter.wasm --json  # as JSON
paglets-host remote describe --connect URL --key KEY --ledger DIR PAGLET
paglets-host remote describe --connect URL --key KEY --ledger DIR --module HASH --json
```

`remote describe` asks the host, which reads the description from the
paglet's module. For a native system paglet (`system.files`,
`system.mesh-info`, …), the host builds one from the service's contract.

```text
interface: counter - A counter whose state is plain C++ objects; memory images move it
launch:    Start
messages:
  increment  both  Increment -> Status  Adds to or multiplies the value; replies with the status
  status  call  none -> Status  The value and the latest notes
  echo  call  Echo -> Echo  Returns the record unchanged (every field type)
  ...
```

The tool is built next to the guests that use it
(`build/<preset>/examples/paglets-interface`) and can also be run by hand:

```text
paglets-interface embed MODULE.wasm OUT.wasm [--description FILE.json] [--schema FILE.schema.json] [--name NAME]
paglets-interface show MODULE.wasm
```

> [!NOTE]
> Embedding a description changes the module's bytes and so its hash.
> Sign and trust the module after it was built, as with any module.
