// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Patterns (WP18): file mobility. A paglet asks the host for a directory of
// a named root (the mesh policy decides, through `grants`) and then either
//
// - shares a file: the host hashes it and gives an artifact capability,
//   a few bytes that move with the paglet; below a directory granted on
//   another host, `save_file` writes the content there, fetched directly
//   from host to host (planning/cpp-content-exchange.md). Files of any size
//   travel this way, and the content leaves its host only where the root's
//   content may go; or
// - reads a small file into its memory (at most what fits, a few MB),
//   moves, and writes it there. Reads mark the paglet with the root (data
//   residency, planning/cpp-residency.md), so the paglet only goes where
//   its content may go.

#pragma once

#include <paglets/patterns/services.hpp>
#include <paglets/tasks.hpp>
#include <paglets/services/files.gen.hpp>
#include <paglets/services/grants.gen.hpp>

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace paglets::patterns {

// A file a paglet carries.
struct CarriedFile {
    std::string name;  // the path it was read from, below the directory
    Bytes data;
};

// File scanning is a thin adapter over the framework's service-task API.
// The task handles progress delivery, cancellation, deadlines and cleanup.
inline tasks::TaskHandle scan_files(Endpoint files, Capability dir, services::files::FindRequest query,
                                             std::function<void(const services::files::ScanBatch&)> batch,
                                             std::function<void(Result<void>)> done,
                                             std::int64_t timeout_ms = 0) {
    tasks::Options options;
    options.timeout_ms = timeout_ms;
    options.start.lend.push_back(dir);
    return tasks::start<services::files::ScanBatch>(files, "scan",
        services::files::ScanRequest{std::move(query), timeout_ms}, std::move(batch), std::move(done), std::move(options));
}

// Convenience for examples that need a small complete list. Accumulation is
// bounded by query.limit (at most 10000) and 512 KB, with explicit truncation.
inline tasks::TaskHandle find_files(Endpoint files, Capability dir, services::files::FindRequest query,
                                             std::function<void(Result<services::files::FindReply>)> done,
                                             std::int64_t timeout_ms = 0) {
    struct State {
        services::files::FindReply result;
        std::size_t bytes = 0;
        std::weak_ptr<tasks::detail::Control> scan;
    };
    auto state = std::make_shared<State>();
    const auto limit = std::clamp(query.limit, 1u, 10000u);
    auto scan = scan_files(files, dir, std::move(query), [state, limit](const services::files::ScanBatch& b) {
        for (const auto& e : b.entries) {
            const auto bytes = e.path.size() + e.name.size() + 128;
            if (state->result.entries.size() >= limit || state->bytes + bytes > 512 * 1024) {
                state->result.truncated = true;
                if (auto s = state->scan.lock()) (void)s->cancel();
                return;
            }
            state->bytes += bytes;
            state->result.entries.push_back(e);
        }
    }, [state, done = std::move(done)](Result<void> r) {
        if (!r && !state->result.truncated) return done(std::unexpected(r.error()));
        std::ranges::sort(state->result.entries, {}, &services::files::Entry::path);
        done(std::move(state->result));
    }, timeout_ms);
    state->scan = scan.weak();
    return scan;
}

// A `dir` capability for `root`/`path` with `rights` (read, write, create,
// delete), granted on this host by the mesh policy.
inline void granted_dir(std::string root, std::string path, std::vector<std::string> rights, std::string reason,
                        std::function<void(Result<Capability>)> next) {
    endpoint("grants", [root = std::move(root), path = std::move(path), rights = std::move(rights),
                        reason = std::move(reason), next](Result<Endpoint> grants) {
        if (!grants) return next(std::unexpected(grants.error()));
        services::grants::Client client{*grants};
        services::grants::Request q;
        q.item = services::grants::Item{"files", rights, root, path};
        q.reason = reason;
        auto sent = client.request(q, [next](Result<services::grants::Answer> a, Message& m) {
            if (!a) return next(std::unexpected(a.error()));
            if (a->status != services::grants::Status::granted || m.cap_count() == 0)
                return next(std::unexpected(a->status == services::grants::Status::pending ? abi::bad_state : abi::denied));
            next(m.take_cap(0));
        });
        if (!sent) next(std::unexpected(sent.error()));
    });
}

namespace detail {

// Leave room for the service record and the enclosing ABI message. A
// document-sized data chunk makes write requests exceed the import limit.
inline constexpr std::size_t file_chunk_size = abi::max_document_size / 2;

struct ReadState {
    Endpoint files;
    Capability dir;
    CarriedFile file;
    std::uint64_t max_bytes = 0;
    std::function<void(Result<CarriedFile>)> next;
};

inline void read_chunk(std::shared_ptr<ReadState> s) {
    services::files::Client client{s->files};
    auto sent = client.read(
        services::files::ReadRequest{s->file.name, s->file.data.size(), file_chunk_size},
        [s](Result<services::files::ReadReply> r, Message&) {
            if (!r) return s->next(std::unexpected(r.error()));
            if (r->size > s->max_bytes) return s->next(std::unexpected(abi::too_large));
            s->file.data.insert(s->file.data.end(), r->data.begin(), r->data.end());
            if (!r->eof && !r->data.empty()) return read_chunk(s);
            s->next(std::move(s->file));
        },
        lending(s->dir));
    if (!sent) s->next(std::unexpected(sent.error()));
}

struct WriteState {
    Endpoint files;
    Capability dir;
    std::string path;
    Bytes data;
    std::size_t at = 0;
    std::function<void(Result<void>)> next;
};

inline void write_chunk(std::shared_ptr<WriteState> s) {
    const std::size_t n = std::min(file_chunk_size, s->data.size() - s->at);
    services::files::WriteRequest q;
    q.path = s->path;
    q.data.assign(s->data.begin() + static_cast<std::ptrdiff_t>(s->at),
                  s->data.begin() + static_cast<std::ptrdiff_t>(s->at + n));
    q.mode = s->at == 0 ? services::files::WriteMode::replace : services::files::WriteMode::append;
    services::files::Client client{s->files};
    auto sent = client.write(
        q,
        [s, n](Result<services::files::Entry> r, Message&) {
            if (!r) return s->next(std::unexpected(r.error()));
            s->at += n;
            if (s->at < s->data.size()) return write_chunk(s);
            s->next({});
        },
        lending(s->dir));
    if (!sent) s->next(std::unexpected(sent.error()));
}

}  // namespace detail

// Reads `path` below the lent directory `dir` into memory (at most
// `max_bytes`).
inline void read_file(const Capability& dir, std::string path, std::uint64_t max_bytes,
                      std::function<void(Result<CarriedFile>)> next) {
    endpoint("files", [dir, path = std::move(path), max_bytes, next](Result<Endpoint> files) {
        if (!files) return next(std::unexpected(files.error()));
        auto s = std::make_shared<detail::ReadState>();
        s->files = *files;
        s->dir = dir;
        s->file.name = path;
        s->max_bytes = max_bytes;
        s->next = next;
        detail::read_chunk(s);
    });
}

// Writes `data` to `path` below the lent directory `dir` (replacing it).
inline void write_file(const Capability& dir, std::string path, Bytes data, std::function<void(Result<void>)> next) {
    endpoint("files", [dir, path = std::move(path), data = std::move(data), next](Result<Endpoint> files) mutable {
        if (!files) return next(std::unexpected(files.error()));
        auto s = std::make_shared<detail::WriteState>();
        s->files = *files;
        s->dir = dir;
        s->path = path;
        s->data = std::move(data);
        s->next = next;
        detail::write_chunk(s);
    });
}

// Picks up `root`/`path` on this host (a read grant for its directory).
inline void pick_up(std::string root, std::string path, std::uint64_t max_bytes,
                    std::function<void(Result<CarriedFile>)> next) {
    const auto slash = path.rfind('/');
    std::string dir_path = slash == std::string::npos ? std::string() : path.substr(0, slash);
    std::string name = slash == std::string::npos ? path : path.substr(slash + 1);
    granted_dir(std::move(root), std::move(dir_path), {"read"}, "carry a file",
                [name = std::move(name), max_bytes, next](Result<Capability> dir) {
                    if (!dir) return next(std::unexpected(dir.error()));
                    auto cap = std::make_shared<Capability>(*dir);
                    read_file(*cap, name, max_bytes, [cap, next](Result<CarriedFile> f) {
                        (void)cap->drop();
                        next(std::move(f));
                    });
                });
}

// Makes the directory `path` (and its parents) below the lent directory;
// one that exists already is fine.
inline void make_dirs(const Capability& dir, std::string path, std::function<void(Result<void>)> next) {
    if (path.empty()) return next({});
    endpoint("files", [dir, path = std::move(path), next](Result<Endpoint> files) {
        if (!files) return next(std::unexpected(files.error()));
        services::files::Client client{*files};
        auto sent = client.mkdir(
            services::files::MkdirRequest{path, true},
            [next](Result<services::files::Entry> r, Message&) {
                if (!r && r.error() != abi::bad_state) return next(std::unexpected(r.error()));
                next({});
            },
            lending(dir));
        if (!sent) next(std::unexpected(sent.error()));
    });
}

// Puts a carried file down as `root`/`path` on this host (write and create
// grants for the root; missing directories are made).
inline void put_down(const CarriedFile& file, std::string root, std::string path,
                     std::function<void(Result<void>)> next) {
    const auto slash = path.rfind('/');
    std::string dir_path = slash == std::string::npos ? std::string() : path.substr(0, slash);
    granted_dir(std::move(root), "", {"write", "create"}, "deliver a file",
                [dir_path = std::move(dir_path), path = std::move(path), data = file.data,
                 next](Result<Capability> dir) mutable {
                    if (!dir) return next(std::unexpected(dir.error()));
                    auto cap = std::make_shared<Capability>(*dir);
                    make_dirs(*cap, dir_path,
                              [cap, path = std::move(path), data = std::move(data), next](Result<void> made) mutable {
                                  if (!made) {
                                      (void)cap->drop();
                                      return next(std::move(made));
                                  }
                                  write_file(*cap, path, std::move(data), [cap, next](Result<void> r) {
                                      (void)cap->drop();
                                      next(std::move(r));
                                  });
                              });
                });
}

// The SHA-256 (hex) and size of `path` below the lent directory `dir`
// (right `read`), computed by the host: files of any size, nothing read
// into the paglet.
inline void hash_file(const Capability& dir, std::string path,
                      std::function<void(Result<services::files::Hashed>)> next) {
    endpoint("files", [dir, path = std::move(path), next](Result<Endpoint> files) {
        if (!files) return next(std::unexpected(files.error()));
        services::files::Client client{*files};
        RequestOptions options = lending(dir);
        options.timeout_ms = 3600 * 1000;  // hashing a large file takes time
        auto sent = client.hash(
            services::files::HashRequest{path},
            [next](Result<services::files::Hashed> r, Message&) { next(std::move(r)); }, options);
        if (!sent) next(std::unexpected(sent.error()));
    });
}

// A file shared for other hosts: its content stays where it is until a
// host that saves or reads it fetches it.
struct SharedFile {
    std::string name;  // the path below the directory
    std::string sha256;
    std::uint64_t size = 0;
    Capability content;  // artifact capability (rights `read`); it moves with the paglet
};

// Shares `path` below the lent directory `dir` (right `read`): the host
// hashes the file and answers with an artifact capability to it. Nothing is
// read into the paglet.
inline void share_file(const Capability& dir, std::string path, std::function<void(Result<SharedFile>)> next) {
    endpoint("files", [dir, path = std::move(path), next](Result<Endpoint> files) {
        if (!files) return next(std::unexpected(files.error()));
        services::files::Client client{*files};
        RequestOptions options = lending(dir);
        // Hashing a large file takes time.
        options.timeout_ms = 3600 * 1000;
        auto sent = client.share(
            services::files::ShareRequest{path, {}},
            [path, next](Result<services::files::Shared> r, Message& m) {
                if (!r) return next(std::unexpected(r.error()));
                if (m.cap_count() == 0) return next(std::unexpected(abi::internal));
                next(SharedFile{path, r->hash, r->size, m.take_cap(0)});
            },
            options);
        if (!sent) next(std::unexpected(sent.error()));
    });
}

// Writes the content of an artifact capability (from `share_file`, or any
// artifact) to `path` below the lent directory `dir` (rights `create`, and
// `write` to replace a file). The host fetches the content from the host
// that holds it and checks its SHA-256 before the file appears.
inline void save_file(const Capability& dir, std::string path, const Capability& content,
                      std::function<void(Result<services::files::Entry>)> next, bool replace = true) {
    endpoint("files", [dir, path = std::move(path), content, replace, next](Result<Endpoint> files) {
        if (!files) return next(std::unexpected(files.error()));
        services::files::Client client{*files};
        RequestOptions options = lending(content, lending(dir));
        // Fetching a large file takes time.
        options.timeout_ms = 3600 * 1000;
        auto sent = client.save(
            services::files::SaveRequest{path, replace},
            [next](Result<services::files::Entry> r, Message&) { next(std::move(r)); }, options);
        if (!sent) next(std::unexpected(sent.error()));
    });
}

}  // namespace paglets::patterns
