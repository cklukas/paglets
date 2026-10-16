# Image Describer

The Image Describer makes image collections searchable by their content.
It reads the images on the hosts that keep them, moves to a host whose
language model can see, gets a caption and a few tags for every image,
and writes a JSON catalogue of all images on another host.

Directory: [`cpp/examples/describer/`](https://github.com/cklukas/paglets/tree/cpp/cpp/examples/describer)


File selection uses the framework's background scan-task helper. The agent
stays responsive while the directory walk runs; bounded list results and
operation deadlines are separate from handler time. See
[Long-running agents and tasks](../long-running-tasks.md) for the shared
`TaskHandle` API, cancellation, progress and the example audit.

## What it shows

- **Offer requirements.** Not every `ai` host can describe images. A host
  whose models include one that can see offers the operation
  `describe_image` and the attribute `vision=yes`. The describer asks
  `mesh-info` for an offer of `ai.describe_image` with the requirement
  `vision = yes`, and moves to the host it names (see
  [Service offers](../system-paglets.md#service-offers)).
- **`ai.describe_image`.** It sends each image with a prompt to the `ai`
  system paglet: once for a caption, once for tags (see
  [Gateways to the web and to AI](../system-paglets.md#gateways-to-the-web-and-to-ai)).
- **Grants on several hosts.** It asks the `grants` system paglet of each
  source host for `read` on a named root, and the output host for `write`
  and `create`. The mesh policy decides.
- **Data residency.** Reading an image marks the paglet with the root it
  came from. Images of a root whose content must stay on its host keep the
  paglet there: the move to the AI host is refused, and no image reaches
  the model (see [Data residency](../hosts-and-meshes.md#data-residency)).

## Why it is useful

Photos, scans and microscope images pile up on many machines with file
names that say little about them. A catalogue with a caption and tags per
image makes them findable with any text search. The describer needs
nothing on the image hosts, uses the one machine with a vision model for
all of them, and leaves images of protected roots where they are.

## How it works

The describer starts with a `start` message that lists the sources (a
host, a named root and a file pattern each), the output file, the model,
and limits. It visits the source hosts one after another, gets a
directory with `read`, and finds the files of the pattern whose names end
in `.png`, `.jpg` or `.jpeg`, in any case. It reads them into its memory.
An image larger than `max_image_bytes`, or one that would take the total
above `max_total_bytes`, is skipped and listed in `skipped`. At most
`max_images` images are read:

<!-- snippet: cpp/examples/describer/describer.cpp#limits -->
Source: [`cpp/examples/describer/describer.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/describer/describer.cpp)

```cpp
void read_next(std::size_t i) {
    const auto root = params_.sources[source_].root;
    while (i < found_.size() &&
           (found_[i].size > static_cast<std::uint64_t>(params_.max_image_bytes) ||
            total_ + static_cast<std::int64_t>(found_[i].size) > params_.max_total_bytes)) {
        status_.skipped.push_back(here_name_ + ": " + root + "/" + found_[i].path + " (too large)");
        ++i;
    }
    if (i >= found_.size() || static_cast<std::int64_t>(pixels_.size()) >= params_.max_images) {
        (void)dir_->drop();
        dir_.reset();
        ++source_;
        return next_source();
    }
    pt::read_file(*dir_, found_[i].path, static_cast<std::uint64_t>(params_.max_image_bytes),
                  [this, i, root](paglets::Result<pt::CarriedFile> f) {
                      if (!f) {
                          status_.skipped.push_back(here_name_ + ": " + root + "/" + found_[i].path + " (unreadable)");
                          return read_next(i + 1);
                      }
                      Image img;
                      img.host = here_name_;
                      img.path = root + "/" + f->name;
                      img.size = static_cast<std::int64_t>(f->data.size());
                      total_ += img.size;
                      status_.images.push_back(std::move(img));
                      pixels_.push_back(std::move(f->data));
                      read_next(i + 1);
                  });
}
```
<!-- /snippet -->

After the last source, it looks for a host whose `ai` offer can see. It
does not use a transfer ticket such as `offer:ai.describe_image` here,
because a ticket cannot express the requirement `vision = yes`;
`mesh-info`'s `find_offers` can. It notes the host's name and moves there:

<!-- snippet: cpp/examples/describer/describer.cpp#offer -->
Source: [`cpp/examples/describer/describer.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/describer/describer.cpp)

```cpp
// A host whose `ai` offer can see: the offer requirement vision=yes.
void to_ai_host() {
    if (pixels_.empty()) {
        status_.state = "writing";
        if (is_here(params_.output.host)) return write();
        return go(params_.output.host);
    }
    auto mi = paglets::service("mesh-info");
    if (!mi) return fail("no mesh-info");
    svc::mesh_info::Client client{*mi};
    svc::mesh_info::OffersRequest q;
    q.service = "ai";
    q.op = "describe_image";
    q.require.push_back(svc::mesh_info::Requirement{"vision", "=", "yes", 0});
    q.limit = 1;
    auto sent = client.find_offers(q, [this](paglets::Result<svc::mesh_info::Offers> o, Message&) {
        if (!o) return fail(pt::error_text("find_offers", o.error()));
        if (o->matches.empty()) return fail("no host offers ai.describe_image with vision");
        const auto& m = o->matches.front();
        status_.ai_host = m.host_name;
        status_.state = "describing";
        if (is_here(m.host)) return describe(0);
        go(m.host);
    });
    if (!sent) fail(pt::error_text("find_offers", sent.error()));
}
```
<!-- /snippet -->

On the AI host, it asks `ai.describe_image` twice per image: for a caption
in one sentence, and for up to eight comma-separated tags, which it splits
into a list. When every image is described, it drops the image data, so
only the text travels on to the output host:

<!-- snippet: cpp/examples/describer/describer.cpp#describe -->
Source: [`cpp/examples/describer/describer.cpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/describer/describer.cpp)

```cpp
void describe(std::size_t i) {
    if (i >= pixels_.size()) {
        pixels_.clear();  // described: only the text travels on
        status_.state = "writing";
        if (is_here(params_.output.host)) return write();
        return go(params_.output.host);
    }
    ask(pixels_[i], "Describe this image in one sentence.", [this, i](paglets::Result<svc::ai::TextReply> caption) {
        if (!caption) return fail(pt::error_text("describe_image", caption.error()));
        status_.images[i].caption = caption->text;
        status_.model = caption->model;
        ask(pixels_[i], "Give up to eight comma-separated tags for this image.",
            [this, i](paglets::Result<svc::ai::TextReply> tags) {
                if (!tags) return fail(pt::error_text("describe_image", tags.error()));
                status_.images[i].tags = tags_of(tags->text);
                describe(i + 1);
            });
    });
}
```
<!-- /snippet -->

On the output host, it asks for the output root with `write` and
`create`, and writes the catalogue: the model, and for every image its
host, path, size, caption and tags. Its state is then `written`, and it
stays on the output host.

The messages and the status:

<!-- snippet: cpp/examples/describer/describer_msgs.hpp -->
Source: [`cpp/examples/describer/describer_msgs.hpp`](https://github.com/cklukas/paglets/blob/cpp/cpp/examples/describer/describer_msgs.hpp)

```cpp
// Messages of the Image Describer demo (planning/cpp-demo-paglets.md, demo 18).

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace describer_msgs {

// Where to look for images: a host (name or key ID), a named root on it and
// a file pattern; files ending in .png, .jpg or .jpeg count.
struct Source {
    std::string host;
    std::string root;
    std::string pattern = "**";
};

// Where the catalogue goes: a JSON file below a named root of a host.
struct Output {
    std::string host;
    std::string root;
    std::string path = "images.json";
};

// `start`: collect the images, describe them on a host whose `ai` offer can
// see (vision=yes, describe_image), write the catalogue.
struct Start {
    std::vector<Source> sources;
    Output output;
    std::string model;  // empty: the AI host's default
    std::int64_t max_images = 50;
    // The images travel in the paglet's memory (within its module/host allowance), and each
    // goes to the `ai` system paglet in one request (by default at most
    // 256 KB of input).
    std::int64_t max_image_bytes = 256 * 1024;
    std::int64_t max_total_bytes = 8 * 1024 * 1024;
};

struct Started {
    bool accepted = false;
    std::string reason;
};

struct Image {
    std::string host;
    std::string path;  // root/path on that host
    std::int64_t size = 0;
    std::string caption;
    std::vector<std::string> tags;
};

struct Status {
    std::string state;  // idle, collecting, describing, writing, written, refused, failed
    std::vector<Image> images;
    std::vector<std::string> skipped;  // too large, unreadable
    std::vector<std::string> hosts;    // names of the hosts it was on, in order
    std::string ai_host;               // the host that described the images
    std::string model;
    std::string error;
};

}  // namespace describer_msgs
```
<!-- /snippet -->

## Running it on a live mesh

The mesh needs hosts with image roots, a host with an output root, and a
host that offers `ai` with a model that can see. With Ollama, a model
counts as one that can see when its families include `clip` or `mllama`,
such as `llava` or `llama3.2-vision`:

```bash
H=build/macos-arm64/host/paglets-host
$H serve --key photos.key --ledger ledger-photos --state state-photos --root pics=/srv/pictures \
    --join https://lab-1:7443 &
$H serve --key archive.key --ledger ledger-archive --state state-archive --root out=/srv/catalogues \
    --join https://lab-1:7443 &
$H serve --key mac.key --ledger ledger-mac --state state-mac --ai ollama --ai-model llava \
    --join https://lab-1:7443 &
```

An admin adds the rules: read `pics`, write and create in `out`, and
describe images with `ai`:

```bash
$H ledger rule --ledger ledger --admin alice.key --name "read images" --decision allow \
    --service files --op read --root pics
$H ledger rule --ledger ledger --admin alice.key --name "write catalogues" --decision allow \
    --service files --op write --op create --root out
$H ledger rule --ledger ledger --admin alice.key --name describe --decision allow \
    --service ai --op describe_image
$H remote push --connect https://lab-1:7443 --key alice.key --ledger ledger
```

Then the owner launches the describer and starts it. `model` names the
vision model; without it, the AI host uses its first model:

```bash
R="--connect https://lab-1:7443 --key olga.key --ledger ledger"
ID=$($H remote launch $R build/macos-arm64/guests/describer.wasm)
$H remote call $R $ID start '{"sources": [{"host": "photos", "root": "pics", "pattern": "2026/**"}],
    "output": {"host": "archive", "root": "out", "path": "images.json"},
    "model": "llava"}'
```

> [!NOTE]
> The images travel in the paglet's memory, which has no operator ceiling by default within Wasm32's address space,
> and each image goes to the `ai` system paglet in one request. That
> system paglet takes at most 256 KB of input per request by default
> (`AiConfig::max_input_bytes`), and a message holds at most 1 MB. The
> defaults of `max_image_bytes` (256 KB) and `max_total_bytes` (8 MB)
> keep within these limits; larger images need smaller versions, or an
> AI host with a larger input limit.

The describer ends on the output host. `remote call` reaches a paglet on
the host where it is, so ask that host for the status (`remote locate`
tells where the paglet is):

```bash
$H remote call --connect https://archive:7443 --key olga.key --ledger ledger $ID status
```

## Test results

The test runs the describer on a mesh of four hosts inside one test
process (`gateway: Image Describer ...` in `cpp/tests/test_gateway.cpp`).
Only `mac` offers `ai`, through the `test` backend, whose model counts as
one that can see. Its fixed answer to an image is `an image of N bytes`.
The root `pics` on `photos` holds `a.png` (1000 bytes), `trip/b.JPG`
(2000 bytes), `huge.png` (5000 bytes) and a text file, and the test sets
`max_image_bytes` to 4096. The test checks that:

- the describer visits `home`, `photos`, `mac` and `archive` in that
  order, ends on `archive` in the state `written`, and names `mac` as the
  host that described the images;
- the two images within the limit are described, `a.png` with the caption
  `an image of 1000 bytes` and the same text as its only tag; the text
  file is left out;
- `huge.png` is listed as skipped because it is too large;
- `images.json` on `archive` holds the entry of `a.png` with its host,
  path, size and caption, and the model `test`;
- a describer of the root `private`, which has the residency rule
  `host-only`, is refused on its way to the AI host: its state is
  `refused`, the reason names data residency, and it stays on `photos`.

<!-- results: describer -->
| Test | Mode | Outcome | Duration | Platform | Recorded | images described | images skipped (too large) | host-only root: kept off the AI host |
|---|---|---|---|---|---|---|---|---|
| gateway: Image Describer reads images on their host, describes them where ai can see, writes a catalogue | in-process | passed | 30.3 s | Darwin arm64, RelWithDebInfo build | 2026-10-16 | 2 | 1 | yes |
| gateway: Image Describer reads images on their host, describes them where ai can see, writes a catalogue | worker processes | passed | 40.4 s | Darwin arm64, RelWithDebInfo build | 2026-10-16 | 2 | 1 | yes |
<!-- /results -->
