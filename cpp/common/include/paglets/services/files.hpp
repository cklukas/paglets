// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Contract of the `files` system paglet (planning/cpp-system-paglets.md).
//
// Every operation acts on a `dir` capability the caller lends with the
// request (RequestOptions::lend, first capability); paths are relative to
// that directory, `/`-separated, without `.` or `..`; "" is the directory
// itself. Rights of the capability: `read` (list, stat, find, read, hash, share),
// `write` (change existing files), `create` (new files and directories),
// `delete` (delete; move needs `delete` and `create`). Sizes are bytes,
// times Unix milliseconds (UTC). Symbolic links are listed but never
// followed out of the directory.

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::services::files {

enum class EntryType { file, directory, symlink, other };

struct Entry {
    std::string name;
    std::string path;  // relative to the lent directory
    EntryType type = EntryType::file;
    std::uint64_t size = 0;
    std::int64_t modified_ms = 0;
};

struct ListRequest {
    std::string path;
};

struct ListReply {
    std::vector<Entry> entries;  // sorted by name
};

struct StatRequest {
    std::string path;
};

// Name patterns: `*` matches within a path segment, `?` one character,
// `**` any number of segments ("**/*.csv").
struct FindRequest {
    std::string path;
    std::string pattern = "**";
    std::optional<std::uint64_t> min_size;
    std::optional<std::uint64_t> max_size;
    std::optional<std::int64_t> modified_after_ms;
    std::optional<std::int64_t> modified_before_ms;
    std::uint32_t limit = 1000;
};

struct FindReply {
    std::vector<Entry> entries;
    bool truncated = false;  // more matches than `limit`
};

// At most 4 MB per request; larger files are read in ranges.
struct ReadRequest {
    std::string path;
    std::uint64_t offset = 0;
    std::optional<std::uint64_t> length;
};

struct ReadReply {
    std::vector<std::uint8_t> data;
    std::uint64_t size = 0;  // of the whole file
    bool eof = false;
};

enum class WriteMode { replace, append, create_new };

struct WriteRequest {
    std::string path;
    std::vector<std::uint8_t> data;
    WriteMode mode = WriteMode::replace;
};

struct MkdirRequest {
    std::string path;
    bool parents = false;
};

struct MoveRequest {
    std::string from;
    std::string to;
};

struct DeleteRequest {
    std::string path;
    bool recursive = false;
};

struct DeleteReply {
    std::uint64_t removed = 0;  // files and directories removed
};

// `hash` computes the SHA-256 of a file on the host (right `read`), so a
// paglet compares files of any size without reading them.
struct HashRequest {
    std::string path;
};

struct Hashed {
    std::string sha256;  // hex
    std::uint64_t size = 0;
};

// `share` makes a file available to every host of the mesh without reading
// it into the paglet: the host hashes the file and replies with an
// `artifact` capability (rights `read`) as the first capability of the
// reply. The capability moves with the paglet and travels in messages; on
// another host, `artifacts.get` reads the content and `save` writes it to a
// file, while the content goes directly from host to host. Needs the right
// `read`. The file must not change until it has been fetched.
struct ShareRequest {
    std::string path;
    std::string media_type;  // informational, for example "text/csv"
};

struct Shared {
    std::string hash;  // hex SHA-256
    std::uint64_t size = 0;
};

// `save` writes the content of an artifact capability (the second lent
// capability, after the directory) to `path`, fetching it from the host
// that holds it if needed; the content is checked against its SHA-256
// before the file appears. Needs `create` for a new file and `write` to
// replace one (`replace`).
struct SaveRequest {
    std::string path;
    bool replace = true;
};

struct Contract {
    static constexpr std::string_view service = "files";
    ListReply list(const ListRequest&);
    Entry stat(const StatRequest&);
    FindReply find(const FindRequest&);
    ReadReply read(const ReadRequest&);
    Entry write(const WriteRequest&);
    Entry mkdir(const MkdirRequest&);
    Entry move(const MoveRequest&);
    DeleteReply remove(const DeleteRequest&);
    Hashed hash(const HashRequest&);
    Shared share(const ShareRequest&);
    Entry save(const SaveRequest&);
};

}  // namespace paglets::services::files
