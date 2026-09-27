// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Contract of the `files` system paglet (planning/cpp-system-paglets.md).
//
// Every operation acts on a `dir` capability the caller lends with the
// request (RequestOptions::lend, first capability); paths are relative to
// that directory, `/`-separated, without `.` or `..`; "" is the directory
// itself. Rights of the capability: `read` (list, stat, find, read),
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
};

}  // namespace paglets::services::files
