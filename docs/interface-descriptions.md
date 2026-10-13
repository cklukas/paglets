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

```json
{
  "interface": 1,
  "name": "counter",
  "summary": "A counter whose state is plain C++ objects; memory images move it",
  "launch": {"type": "Start", "example": {"value": 0}},
  "messages": [
    {"name": "increment", "kind": "both", "summary": "Adds to or multiplies the value; replies with the status",
     "request": "Increment", "reply": "Status", "example": {"by": 1, "note": "from the web", "mode": "add"}},
    {"name": "status", "kind": "call", "summary": "The value and the latest notes", "request": "none", "reply": "Status"}
  ],
  "types": [
    {"name": "Start", "kind": "record", "fields": [
      {"name": "value", "type": "i64", "doc": "The value to begin with", "default": 0}]},
    {"name": "Mode", "kind": "enum", "values": ["add", "multiply"]}
  ]
}
```

| Member | Meaning |
|---|---|
| `interface` | the format version, `1` |
| `name`, `summary` | what the module is, in a few words |
| `launch.type` | the type of the start arguments (`--args`), `none` if there are none; `launch.example` fills the form |
| `messages[].name` | the message name the paglet's router handles; names starting with `paglets.` are reserved |
| `messages[].kind` | `call` (a request with a reply), `send` (one-way) or `both`; default `call` |
| `messages[].request`, `messages[].reply` | payload types; `none` for no payload |
| `messages[].summary` | one line about what the message does |
| `messages[].danger` | `true`: tools ask before they send it (for example a message that ends work) |
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
description defines itself takes precedence, which is how fields get
`doc` and `default`), checks the description, and writes it into the
module. A description that names an unknown type, repeats a message or
uses another `kind` fails the build with the reason.

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
