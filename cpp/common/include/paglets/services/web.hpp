// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Contract of the `web` system paglet (planning/cpp-web-ai.md): mediated web
// access on hosts allowed to reach the internet or specific sites. Paglets
// never get sockets; they move to a host that offers `web` and ask it.
//
// - Every request names an absolute http(s) URL. The mesh policy decides
//   per operation and destination (rules and grants for the service `web`
//   with the URL's host as the root and its path as the path).
// - Internal destinations (loopback, private, link-local and similar
//   addresses) are refused unless the host's configuration allows them;
//   every redirect is checked again.
// - GET (and HEAD for `fetch`) only: no uploads.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::services::web {

struct CapabilitiesRequest {};

struct Capabilities {
    bool internet = true;                // public destinations are reachable
    std::vector<std::string> internal;   // URL prefixes of internal destinations allowed here
    bool search = false;                 // a search backend is configured
    std::int64_t max_bytes = 0;          // largest body `fetch` returns
    std::int64_t max_download_bytes = 0; // largest `download`
    bool proxy = false;                  // requests go through a proxy
};

struct Header {
    std::string name;
    std::string value;
};

struct FetchRequest {
    std::string url;
    std::string method = "GET";  // GET or HEAD
    std::int64_t max_bytes = 0;  // 0: the host's limit
};

struct FetchReply {
    std::int64_t status = 0;
    std::string url;  // after redirects
    std::string content_type;
    std::vector<Header> headers;
    std::vector<std::uint8_t> body;
    bool truncated = false;  // the body was longer than allowed
};

struct ExtractRequest {
    std::string url;
};

struct Link {
    std::string url;  // absolute
    std::string text;
};

struct ExtractReply {
    std::int64_t status = 0;
    std::string url;
    std::string title;
    std::string text;  // readable text of the page
    std::vector<Link> links;
};

// Stores the body as an artifact; the reply carries an `artifact`
// capability (rights `read`) as its first capability.
struct DownloadRequest {
    std::string url;
    std::string sha256;  // expected content hash (hex); empty: not checked
};

struct DownloadReply {
    std::int64_t status = 0;
    std::string url;
    std::string artifact;  // hex SHA-256 of the content
    std::int64_t size = 0;
    std::string content_type;
};

struct SearchRequest {
    std::string query;
    std::int64_t limit = 10;
};

struct SearchResult {
    std::string title;
    std::string url;
    std::string snippet;
};

struct SearchReply {
    std::vector<SearchResult> results;
};

struct Contract {
    static constexpr std::string_view service = "web";
    Capabilities capabilities(const CapabilitiesRequest&);
    FetchReply fetch(const FetchRequest&);
    ExtractReply extract_text(const ExtractRequest&);
    DownloadReply download(const DownloadRequest&);
    SearchReply search(const SearchRequest&);
};

}  // namespace paglets::services::web
