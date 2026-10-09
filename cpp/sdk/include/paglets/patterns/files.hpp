// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Patterns (WP18): single-file mobility. A paglet asks the host for a
// directory of a named root (the mesh policy decides, through `grants`),
// reads a file into its memory, moves, and writes it below a directory
// granted on the other host. Reads mark the paglet with the root (data
// residency, planning/cpp-residency.md), so a file from a restricted root
// only goes where its content may go.

#pragma once

#include <paglets/patterns/services.hpp>
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

struct ReadState {
    Endpoint files;
    Capability dir;
    CarriedFile file;
    std::uint64_t max_bytes = 0;
    std::function<void(Result<CarriedFile>)> next;
};

inline void read_chunk(std::shared_ptr<ReadState> s) {
    constexpr std::uint64_t chunk = 1024 * 1024;
    services::files::Client client{s->files};
    auto sent = client.read(
        services::files::ReadRequest{s->file.name, s->file.data.size(), chunk},
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
    constexpr std::size_t chunk = 1024 * 1024;
    const std::size_t n = std::min(chunk, s->data.size() - s->at);
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

}  // namespace paglets::patterns
