// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Path patterns shared by `files.find` and policy scopes: `*` matches within
// a segment, `?` one character, `**` any number of segments (including
// none), segments are separated by `/`.

#pragma once

#include <algorithm>
#include <string_view>
#include <vector>

namespace paglets::glob {

inline std::vector<std::string_view> segments(std::string_view path) {
    std::vector<std::string_view> out;
    std::size_t start = 0;
    while (start <= path.size()) {
        const std::size_t end = std::min(path.find('/', start), path.size());
        if (end > start) out.push_back(path.substr(start, end - start));
        start = end + 1;
    }
    return out;
}

inline bool match_segment(std::string_view pattern, std::string_view text) {
    std::size_t p = 0;
    std::size_t t = 0;
    std::size_t star = std::string_view::npos;
    std::size_t mark = 0;
    while (t < text.size()) {
        if (p < pattern.size() && (pattern[p] == '?' || pattern[p] == text[t])) {
            ++p;
            ++t;
        } else if (p < pattern.size() && pattern[p] == '*') {
            star = p++;
            mark = t;
        } else if (star != std::string_view::npos) {
            p = star + 1;
            t = ++mark;
        } else {
            return false;
        }
    }
    while (p < pattern.size() && pattern[p] == '*') ++p;
    return p == pattern.size();
}

inline bool match_segments(const std::vector<std::string_view>& pattern, std::size_t pi,
                           const std::vector<std::string_view>& path, std::size_t si) {
    if (pi == pattern.size()) return si == path.size();
    if (pattern[pi] == "**") {
        for (std::size_t k = si; k <= path.size(); ++k) {
            if (match_segments(pattern, pi + 1, path, k)) return true;
        }
        return false;
    }
    if (si == path.size() || !match_segment(pattern[pi], path[si])) return false;
    return match_segments(pattern, pi + 1, path, si + 1);
}

// True if `path` matches `pattern` ("docs/**" matches "docs" and below).
inline bool match(std::string_view pattern, std::string_view path) {
    return match_segments(segments(pattern), 0, segments(path), 0);
}

}  // namespace paglets::glob
