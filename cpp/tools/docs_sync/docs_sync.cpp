// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// paglets-docs-sync: keeps generated parts of the documentation (docs/*.md)
// current. Between HTML comment markers, which ckdocs drops, it writes
//
//   <!-- snippet: PATH -->  or  <!-- snippet: PATH#SECTION -->  ... <!-- /snippet -->
//       the file PATH (relative to the repository root), or the lines
//       between `--8<-- [start:SECTION]` and `--8<-- [end:SECTION]` in it,
//       as a code block with a link to the source;
//
//   <!-- results: TOPIC[,TOPIC...] -->  ...  <!-- /results -->
//       the results recorded in docs/results/TOPIC.json: by the tests
//       (PAGLETS_RESULTS_DIR, tests/test.hpp) or by live benchmarks
//       (cpp/tools/mesh_bench.sh).
//
// Usage: paglets-docs-sync [--root DIR] [--check]
//   --root   the repository root (default: the current directory)
//   --check  change nothing; exit 1 if a page is not current

#include <paglets/wire/json.hpp>

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <filesystem>
#include <functional>
#include <fstream>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;
using paglets::wire::Json;

namespace {

constexpr std::string_view source_url = "https://github.com/cklukas/paglets/blob/cpp/";

struct Failure {
    std::string message;
};

std::string read_file(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    if (!in) throw Failure{"cannot read " + p.string()};
    return std::string(std::istreambuf_iterator<char>(in), {});
}

std::vector<std::string> lines_of(const std::string& text) {
    std::vector<std::string> lines;
    std::istringstream in(text);
    for (std::string line; std::getline(in, line);) lines.push_back(line);
    return lines;
}

std::string trim(std::string_view s) {
    const auto b = s.find_first_not_of(" \t");
    if (b == std::string_view::npos) return {};
    const auto e = s.find_last_not_of(" \t\r");
    return std::string(s.substr(b, e - b + 1));
}

std::string language_of(const fs::path& p) {
    const std::string ext = p.extension().string();
    const std::string name = p.filename().string();
    if (ext == ".cpp" || ext == ".hpp" || ext == ".h" || ext == ".c") return "cpp";
    if (ext == ".cmake" || name == "CMakeLists.txt") return "cmake";
    if (ext == ".sh") return "bash";
    if (ext == ".json") return "json";
    if (ext == ".yml" || ext == ".yaml") return "yaml";
    return "text";
}

// -- snippets ------------------------------------------------------------------------------------

std::vector<std::string> snippet_lines(const fs::path& root, const std::string& spec) {
    const auto hash = spec.find('#');
    const std::string path = spec.substr(0, hash);
    const std::string section = hash == std::string::npos ? std::string() : spec.substr(hash + 1);
    const auto all = lines_of(read_file(root / path));
    std::vector<std::string> out;
    bool inside = section.empty();
    bool found = section.empty();
    for (const auto& line : all) {
        const bool marker = line.find("--8<-- [") != std::string::npos;
        if (!section.empty() && line.find("--8<-- [start:" + section + "]") != std::string::npos) {
            inside = found = true;
            continue;
        }
        if (!section.empty() && line.find("--8<-- [end:" + section + "]") != std::string::npos) {
            inside = false;
            continue;
        }
        if (inside && !marker) out.push_back(line);
    }
    if (!found) throw Failure{path + " has no section " + section};
    // Without the license header of a whole file.
    if (section.empty()) {
        while (!out.empty() && (out.front().starts_with("// Copyright") || out.front().starts_with("// Licensed") ||
                                out.front().starts_with("# Copyright") || out.front().starts_with("# Licensed") ||
                                trim(out.front()).empty())) {
            out.erase(out.begin());
        }
    }
    while (!out.empty() && trim(out.back()).empty()) out.pop_back();
    // Common indentation goes.
    std::size_t indent = std::string::npos;
    for (const auto& line : out) {
        if (trim(line).empty()) continue;
        indent = std::min(indent, line.find_first_not_of(' '));
    }
    if (indent != std::string::npos && indent > 0) {
        for (auto& line : out) line = line.size() >= indent ? line.substr(indent) : std::string();
    }
    return out;
}

std::string render_snippet(const fs::path& root, const std::string& spec) {
    const std::string path = spec.substr(0, spec.find('#'));
    std::string out = "Source: [`" + path + "`](" + std::string(source_url) + path + ")\n\n";
    out += "```" + language_of(path) + "\n";
    for (const auto& line : snippet_lines(root, spec)) out += line + "\n";
    out += "```\n";
    return out;
}

// -- results -------------------------------------------------------------------------------------

std::string str(const Json& j, std::string_view key) {
    const Json* v = j.find(key);
    if (v == nullptr) return {};
    if (v->is_string()) return v->as_string();
    if (v->is_number()) return v->number_text();
    return {};
}

std::string cell(std::string s) {
    std::string out;
    for (char c : s) {
        if (c == '|') {
            out += "\\|";
        } else if (c != '\n') {
            out += c;
        }
    }
    return out;
}

std::string duration(const std::string& seconds) {
    if (seconds.empty()) return {};
    double s = 0;
    try {
        s = std::stod(seconds);
    } catch (...) {
        return seconds;
    }
    char buf[32];
    if (s < 60) {
        std::snprintf(buf, sizeof buf, "%.1f s", s);
    } else {
        std::snprintf(buf, sizeof buf, "%d min %02d s", static_cast<int>(s) / 60, static_cast<int>(s) % 60);
    }
    return buf;
}

// Texts in order, with runs of digits compared as numbers ("16 MB" before
// "256 MB").
int natural_compare(std::string_view a, std::string_view b) {
    std::size_t i = 0, j = 0;
    while (i < a.size() && j < b.size()) {
        if (std::isdigit(static_cast<unsigned char>(a[i])) && std::isdigit(static_cast<unsigned char>(b[j]))) {
            std::size_t ei = i, ej = j;
            while (ei < a.size() && std::isdigit(static_cast<unsigned char>(a[ei]))) ++ei;
            while (ej < b.size() && std::isdigit(static_cast<unsigned char>(b[ej]))) ++ej;
            const std::string_view na = a.substr(i, ei - i), nb = b.substr(j, ej - j);
            if (na.size() != nb.size()) return na.size() < nb.size() ? -1 : 1;
            if (na != nb) return na < nb ? -1 : 1;
            i = ei;
            j = ej;
            continue;
        }
        if (a[i] != b[j]) return a[i] < b[j] ? -1 : 1;
        ++i;
        ++j;
    }
    return a.size() - i == b.size() - j ? 0 : (a.size() - i < b.size() - j ? -1 : 1);
}

// A number with at most two decimals (without trailing zeros); other text
// as it is.
std::string rounded(const std::string& value) {
    const auto dot = value.find('.');
    if (dot == std::string::npos || value.size() - dot <= 3) return value;
    double d = 0;
    try {
        std::size_t used = 0;
        d = std::stod(value, &used);
        if (used != value.size()) return value;
    } catch (...) {
        return value;
    }
    char buf[64];
    std::snprintf(buf, sizeof buf, "%.2f", d);
    std::string out = buf;
    while (out.back() == '0') out.pop_back();
    if (out.back() == '.') out.pop_back();
    return out;
}

// The header of a measured value: its name, with the unit.
std::string value_header(const Json& item) {
    const std::string unit = str(item, "unit");
    return unit.empty() ? str(item, "name") : str(item, "name") + " (" + unit + ")";
}

std::string render_results(const fs::path& root, const std::string& topics) {
    std::vector<Json> tests;
    std::vector<std::string> missing;
    std::stringstream list(topics);
    for (std::string topic; std::getline(list, topic, ',');) {
        topic = trim(topic);
        const fs::path file = root / "docs" / "results" / (topic + ".json");
        if (!fs::exists(file)) {
            missing.push_back(topic);
            continue;
        }
        auto j = Json::parse(read_file(file));
        if (!j || !j->find("tests") || !j->find("tests")->is_array()) throw Failure{"malformed " + file.string()};
        for (const auto& t : j->find("tests")->as_array()) tests.push_back(t);
    }
    if (tests.empty()) {
        return "> [!NOTE]\n> No results are recorded yet. Run `cpp/tools/update_results.sh` (see\n"
               "> [Example paglets](index.md#test-results)).\n";
    }
    std::ranges::stable_sort(tests, [](const Json& a, const Json& b) {
        const int c = natural_compare(str(a, "test"), str(b, "test"));
        return c != 0 ? c < 0 : str(a, "mode") < str(b, "mode");
    });
    // Columns that are empty in every row are left out.
    struct Column {
        std::string title;
        std::function<std::string(const Json&)> text;
    };
    const std::vector<Column> columns{
        {"Test", [](const Json& t) { return str(t, "test"); }},
        {"Mode", [](const Json& t) { return str(t, "mode"); }},
        {"Outcome", [](const Json& t) { return str(t, "outcome"); }},
        {"Duration",
         [](const Json& t) {
             const std::string s = str(t, "seconds");
             return s.empty() || s == "0" ? std::string() : duration(s);
         }},
        {"Platform",
         [](const Json& t) {
             std::string platform = str(t, "platform");
             if (const auto build = str(t, "build"); !build.empty()) {
                 platform += (platform.empty() ? "" : ", ") + build + " build";
             }
             return platform;
         }},
        {"Recorded", [](const Json& t) { return str(t, "date"); }},
    };
    std::vector<const Column*> shown;
    for (const auto& c : columns) {
        if (std::ranges::any_of(tests, [&](const Json& t) { return !c.text(t).empty(); })) shown.push_back(&c);
    }
    // When every entry measured the same values, they become columns.
    auto headers_of = [](const Json& t) {
        std::vector<std::string> h;
        if (const Json* v = t.find("values"); v != nullptr && v->is_array()) {
            for (const auto& item : v->as_array()) h.push_back(value_header(item));
        }
        return h;
    };
    const auto pivot = headers_of(tests.front());
    const bool pivoted = !pivot.empty() && pivot.size() <= 6 &&
                         std::ranges::all_of(tests, [&](const Json& t) { return headers_of(t) == pivot; });
    std::string out = "|";
    std::string rule = "|";
    for (const auto* c : shown) {
        out += " " + c->title + " |";
        rule += "---|";
    }
    if (pivoted) {
        for (const auto& h : pivot) {
            out += " " + cell(h) + " |";
            rule += "---|";
        }
    }
    out += "\n" + rule + "\n";
    for (const auto& t : tests) {
        out += "|";
        for (const auto* c : shown) out += " " + cell(c->text(t)) + " |";
        if (pivoted) {
            for (const auto& item : t.find("values")->as_array()) out += " " + cell(rounded(str(item, "value"))) + " |";
        }
        out += "\n";
    }
    if (pivoted) return out;
    // Otherwise one small table per test: a row per measured value, a
    // column per mode.
    std::vector<std::string> names;
    for (const auto& t : tests) {
        if (std::ranges::find(names, str(t, "test")) == names.end()) names.push_back(str(t, "test"));
    }
    for (const auto& name : names) {
        std::vector<std::string> modes;
        std::vector<std::string> rows;
        std::map<std::pair<std::string, std::string>, std::string> at;  // (row, mode) -> value
        for (const auto& t : tests) {
            if (str(t, "test") != name) continue;
            const Json* v = t.find("values");
            if (v == nullptr || !v->is_array() || v->as_array().empty()) continue;
            const std::string mode = str(t, "mode");
            if (std::ranges::find(modes, mode) == modes.end()) modes.push_back(mode);
            for (const auto& item : v->as_array()) {
                const std::string row = str(item, "name");
                if (std::ranges::find(rows, row) == rows.end()) rows.push_back(row);
                std::string value = rounded(str(item, "value"));
                if (const auto unit = str(item, "unit"); !unit.empty()) value += " " + unit;
                at[{row, mode}] = value;
            }
        }
        if (rows.empty()) continue;
        out += "\n" + cell(name) + ":\n\n| Measured |";
        std::string head_rule = "|---|";
        for (const auto& m : modes) {
            out += " " + cell(m.empty() ? std::string("value") : m) + " |";
            head_rule += "---|";
        }
        out += "\n" + head_rule + "\n";
        for (const auto& r : rows) {
            out += "| " + cell(r) + " |";
            for (const auto& m : modes) {
                auto it = at.find({r, m});
                out += " " + (it == at.end() ? std::string() : cell(it->second)) + " |";
            }
            out += "\n";
        }
    }
    return out;
}

// -- pages ---------------------------------------------------------------------------------------

std::optional<std::string> marker_argument(const std::string& line, std::string_view kind) {
    const std::string t = trim(line);
    const std::string open = "<!-- " + std::string(kind) + ":";
    if (!t.starts_with(open) || !t.ends_with("-->")) return std::nullopt;
    return trim(t.substr(open.size(), t.size() - open.size() - 3));
}

std::string sync_page(const fs::path& root, const fs::path& page) {
    const auto lines = lines_of(read_file(page));
    std::string out;
    for (std::size_t i = 0; i < lines.size(); ++i) {
        const std::string& line = lines[i];
        std::optional<std::string> snippet = marker_argument(line, "snippet");
        std::optional<std::string> results = marker_argument(line, "results");
        if (!snippet && !results) {
            out += line + "\n";
            continue;
        }
        const std::string close = snippet ? "<!-- /snippet -->" : "<!-- /results -->";
        std::size_t end = i + 1;
        while (end < lines.size() && trim(lines[end]) != close) ++end;
        if (end == lines.size()) throw Failure{page.string() + ": " + trim(line) + " has no " + close};
        try {
            out += line + "\n" + (snippet ? render_snippet(root, *snippet) : render_results(root, *results)) + close +
                   "\n";
        } catch (const Failure& f) {
            throw Failure{page.string() + ": " + f.message};
        }
        i = end;
    }
    return out;
}

}  // namespace

int main(int argc, char** argv) {
    fs::path root = fs::current_path();
    bool check = false;
    for (int i = 1; i < argc; ++i) {
        const std::string_view a = argv[i];
        if (a == "--check") {
            check = true;
        } else if (a == "--root" && i + 1 < argc) {
            root = argv[++i];
        } else {
            std::cerr << "usage: paglets-docs-sync [--root DIR] [--check]\n";
            return 2;
        }
    }
    try {
        if (!fs::is_directory(root / "docs")) throw Failure{"no docs/ below " + root.string()};
        std::vector<fs::path> pages;
        for (const auto& e : fs::recursive_directory_iterator(root / "docs")) {
            if (e.is_regular_file() && e.path().extension() == ".md") pages.push_back(e.path());
        }
        std::ranges::sort(pages);
        int stale = 0;
        for (const auto& page : pages) {
            const std::string before = read_file(page);
            const std::string after = sync_page(root, page);
            if (after == before) continue;
            ++stale;
            if (check) {
                std::cerr << "not current: " << fs::relative(page, root).string() << "\n";
            } else {
                std::ofstream(page, std::ios::binary) << after;
                std::cout << "updated " << fs::relative(page, root).string() << "\n";
            }
        }
        if (check && stale > 0) {
            std::cerr << stale << " page(s) not current; run paglets-docs-sync from the repository root\n";
            return 1;
        }
        return 0;
    } catch (const Failure& f) {
        std::cerr << "paglets-docs-sync: " << f.message << "\n";
        return 1;
    }
}
