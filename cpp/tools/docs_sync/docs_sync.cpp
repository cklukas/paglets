// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// paglets-docs-sync: keeps generated parts of the documentation (docs/*.md)
// and of the papers (papers/**/*.tex) current. Between markers (HTML
// comments, which ckdocs drops; LaTeX comments) it writes
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
// In LaTeX the markers are `% snippet: SPEC [OPTIONS]` ... `% /snippet` and
// `% results: TOPICS [OPTIONS]` ... `% /results`. A snippet becomes a
// `lstlisting` of style `paglets-<language>` with OPTIONS as further
// listings options (for example `[float,caption={...},label={lst:x}]`; a
// caption gets the source path appended as `\src{PATH}`). Results become
// booktabs tabulars inside a `table` with OPTIONS `caption={...}` and
// `label={...}`. The paper's preamble defines `\src` and the styles.
// Results may select `layout={compact}` or `layout={transpose}` with
// `columns={field|measured name|...}`, optional `headers={title|...}`,
// `test-prefix={...}`, `strip-prefix={prefix|...}`, `header={Host}` and
// `widths={0.2|0.3|...}` (fractions of linewidth). Unknown columns fail.
//
// Usage: paglets-docs-sync [--root DIR] [--check] [--only FILE]...
//   --root   the repository root (default: the current directory)
//   --check  change nothing; exit 1 if a page is not current
//   --only   process only these files (relative to the root)

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

// -- LaTeX ---------------------------------------------------------------------------------------

// Text in LaTeX: the special characters escaped.
std::string latex(std::string_view text) {
    std::string out;
    for (std::size_t i = 0; i < text.size(); ++i) {
        if (text.substr(i).starts_with("→")) {
            out += "$\\rightarrow$";
            i += 2;
            continue;
        }
        const char c = text[i];
        switch (c) {
            case '\\': out += "\\textbackslash{}"; break;
            case '&': case '%': case '$': case '#': case '_': case '{': case '}':
                out += '\\';
                out += c;
                break;
            case '~': out += "\\textasciitilde{}"; break;
            case '^': out += "\\textasciicircum{}"; break;
            case '\n': out += ' '; break;
            default: out += c;
        }
    }
    return out;
}

// The argument of a LaTeX marker: the spec, and the options in [...].
struct TexArgument {
    std::string spec;
    std::string options;
};

TexArgument split_tex_argument(const std::string& argument) {
    const auto bracket = argument.find('[');
    if (bracket == std::string::npos) return {trim(argument), {}};
    std::string options = trim(argument.substr(bracket + 1));
    if (!options.empty() && options.back() == ']') options.pop_back();
    return {trim(argument.substr(0, bracket)), trim(options)};
}

// The value of `key={...}` in listings/table options, if given.
std::optional<std::string> option_value(const std::string& options, std::string_view key) {
    const std::string needle = std::string(key) + "={";
    const auto at = options.find(needle);
    if (at == std::string::npos) return std::nullopt;
    int depth = 0;
    std::string value;
    for (std::size_t i = at + needle.size() - 1; i < options.size(); ++i) {
        if (options[i] == '{') {
            if (depth++ == 0) continue;
        } else if (options[i] == '}') {
            if (--depth == 0) return value;
        }
        value += options[i];
    }
    return std::nullopt;
}

std::string render_snippet_tex(const fs::path& root, const std::string& argument) {
    const auto [spec, options] = split_tex_argument(argument);
    const std::string path = spec.substr(0, spec.find('#'));
    std::string opts = "style=paglets-" + language_of(path);
    if (!options.empty()) {
        // The source path goes into the caption.
        std::string o = options;
        if (auto caption = option_value(o, "caption")) {
            const std::string old = "caption={" + *caption + "}";
            o.replace(o.find(old), old.size(), "caption={" + *caption + " \\src{" + latex(path) + "}}");
        }
        opts += "," + o;
    }
    std::string out = "\\begin{lstlisting}[" + opts + "]\n";
    for (const auto& line : snippet_lines(root, spec)) out += line + "\n";
    out += "\\end{lstlisting}\n";
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

// The results of topics as tables: one wide table (every entry measured
// the same values, which become columns), or a table per test (a row per
// measured value, a column per mode).
struct Table {
    std::string title;                           // empty for the wide table
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> rows;
};

std::vector<Json> result_tests(const fs::path& root, const std::string& topics) {
    std::vector<Json> tests;
    std::stringstream list(topics);
    for (std::string topic; std::getline(list, topic, ',');) {
        topic = trim(topic);
        const fs::path file = root / "docs" / "results" / (topic + ".json");
        if (!fs::exists(file)) continue;
        auto j = Json::parse(read_file(file));
        if (!j || !j->find("tests") || !j->find("tests")->is_array()) throw Failure{"malformed " + file.string()};
        for (const auto& t : j->find("tests")->as_array()) tests.push_back(t);
    }
    return tests;
}

std::vector<Table> result_tables(const fs::path& root, const std::string& topics) {
    auto tests = result_tests(root, topics);
    if (tests.empty()) return {};
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
    std::vector<Table> tables;
    Table wide;
    for (const auto* c : shown) wide.columns.push_back(c->title);
    if (pivoted) {
        for (const auto& h : pivot) wide.columns.push_back(h);
    }
    for (const auto& t : tests) {
        std::vector<std::string> row;
        for (const auto* c : shown) row.push_back(c->text(t));
        if (pivoted) {
            for (const auto& item : t.find("values")->as_array()) row.push_back(rounded(str(item, "value")));
        }
        wide.rows.push_back(std::move(row));
    }
    tables.push_back(std::move(wide));
    if (pivoted) return tables;
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
        Table per;
        per.title = name;
        per.columns.push_back("Measured");
        for (const auto& m : modes) per.columns.push_back(m.empty() ? std::string("value") : m);
        for (const auto& r : rows) {
            std::vector<std::string> row{r};
            for (const auto& m : modes) {
                auto it = at.find({r, m});
                row.push_back(it == at.end() ? std::string() : it->second);
            }
            per.rows.push_back(std::move(row));
        }
        tables.push_back(std::move(per));
    }
    return tables;
}

std::string render_results(const fs::path& root, const std::string& topics) {
    const auto tables = result_tables(root, topics);
    if (tables.empty()) {
        return "> [!NOTE]\n> No results are recorded yet. Run `cpp/tools/update_results.sh` (see\n"
               "> [Example paglets](index.md#test-results)).\n";
    }
    std::string out;
    for (const auto& t : tables) {
        if (!t.title.empty()) out += "\n" + cell(t.title) + ":\n\n";
        out += "|";
        std::string rule = "|";
        for (const auto& c : t.columns) {
            out += " " + cell(c) + " |";
            rule += "---|";
        }
        out += "\n" + rule + "\n";
        for (const auto& row : t.rows) {
            out += "|";
            for (const auto& c : row) out += " " + cell(c) + " |";
            out += "\n";
        }
    }
    return out;
}

std::vector<std::string> result_options_list(const std::string& text) {
    std::vector<std::string> items;
    std::stringstream stream(text);
    for (std::string item; std::getline(stream, item, '|');) items.push_back(trim(item));
    return items;
}

std::optional<Table> selected_results(const fs::path& root, const std::string& topics, const std::string& options) {
    const auto layout = option_value(options, "layout");
    if (!layout) return std::nullopt;
    if (*layout != "compact" && *layout != "transpose") throw Failure{"unknown results layout: " + *layout};
    const auto selected = option_value(options, "columns");
    if (!selected) throw Failure{"compact/transposed results require columns={name|name|...}"};
    const auto columns = result_options_list(*selected);
    if (columns.empty()) throw Failure{"results columns are empty"};
    auto tests = result_tests(root, topics);
    if (auto prefix = option_value(options, "test-prefix"))
        std::erase_if(tests, [&](const Json& t) { return !str(t, "test").starts_with(*prefix); });
    if (tests.empty()) return Table{};
    std::ranges::stable_sort(tests, [](const Json& a, const Json& b) {
        const auto order = natural_compare(str(a, "test"), str(b, "test"));
        return order != 0 ? order < 0 : str(a, "mode") < str(b, "mode");
    });
    const auto strip = result_options_list(option_value(options, "strip-prefix").value_or(""));
    auto name_of = [&](const Json& t) {
        auto name = str(t, "test");
        for (const auto& prefix : strip) if (name.starts_with(prefix)) name = trim(name.substr(prefix.size()));
        return name;
    };
    const std::set<std::string> metadata{"test", "mode", "outcome", "seconds", "platform", "build", "date"};
    auto value_of = [&](const Json& t, const std::string& column) {
        if (column == "test") return name_of(t);
        if (metadata.contains(column)) return rounded(str(t, column));
        if (const auto* values = t.find("values"); values && values->is_array())
            for (const auto& item : values->as_array()) if (str(item, "name") == column) return rounded(str(item, "value"));
        return std::string();
    };
    std::vector<std::string> headers = columns;
    for (std::size_t i = 0; i < columns.size(); ++i) {
        bool known = metadata.contains(columns[i]);
        for (const auto& t : tests) {
            if (const auto* values = t.find("values"); values && values->is_array())
                for (const auto& item : values->as_array()) if (str(item, "name") == columns[i]) {
                    known = true;
                    headers[i] = value_header(item);
                }
        }
        if (!known) throw Failure{"unknown results column: " + columns[i]};
    }
    if (auto labels = option_value(options, "headers")) {
        headers = result_options_list(*labels);
        if (headers.size() != columns.size()) throw Failure{"results headers/columns differ in length"};
    }
    Table table;
    if (*layout == "compact") {
        table.columns = headers;
        for (const auto& t : tests) {
            std::vector<std::string> row;
            for (const auto& column : columns) row.push_back(value_of(t, column));
            table.rows.push_back(std::move(row));
        }
    } else {
        table.columns = {option_value(options, "header").value_or("Measured")};
        for (const auto& t : tests) {
            auto name = name_of(t);
            if (std::ranges::count_if(tests, [&](const Json& other) { return str(other, "test") == str(t, "test"); }) > 1)
                name += " / " + str(t, "mode");
            table.columns.push_back(std::move(name));
        }
        for (std::size_t i = 0; i < columns.size(); ++i) {
            std::vector<std::string> row{headers[i]};
            for (const auto& t : tests) row.push_back(value_of(t, columns[i]));
            table.rows.push_back(std::move(row));
        }
    }
    return table;
}

std::string render_results_tex(const fs::path& root, const std::string& argument) {
    const auto [topics, options] = split_tex_argument(argument);
    auto tables = result_tables(root, topics);
    const bool selected = option_value(options, "layout").has_value();
    if (auto table = selected_results(root, topics, options)) tables = table->columns.empty() ? std::vector<Table>{} : std::vector<Table>{std::move(*table)};
    if (!selected) if (auto header = option_value(options, "header"))
        for (auto& table : tables) if (!table.columns.empty() && table.columns.front() == "Test") table.columns.front() = *header;
    if (tables.empty()) return "% no results are recorded yet (cpp/tools/update_results.sh)\n";
    std::string out = "\\begin{table}[htbp]\n\\centering\\small\n";
    if (auto caption = option_value(options, "caption")) out += "\\caption{" + *caption + "}\n";
    if (auto label = option_value(options, "label")) out += "\\label{" + *label + "}\n";
    for (const auto& t : tables) {
        if (!t.title.empty()) out += "\\par\\medskip\\noindent\\textit{" + latex(t.title) + "}\\par\\smallskip\n";
        std::string spec;
        const auto widths = result_options_list(option_value(options, "widths").value_or(""));
        if (!widths.empty() && widths.size() != t.columns.size()) throw Failure{"results widths/columns differ in length"};
        for (std::size_t i = 0; i < t.columns.size(); ++i) {
            if (!widths.empty()) {
                if (!std::ranges::all_of(widths[i], [](unsigned char c) { return std::isdigit(c) || c == '.'; }))
                    throw Failure{"invalid results width"};
                std::size_t used = 0;
                double width = 0;
                try { width = std::stod(widths[i], &used); } catch (...) { throw Failure{"invalid results width"}; }
                if (used != widths[i].size() || width <= 0 || width > 1) throw Failure{"invalid results width"};
                spec += "p{" + widths[i] + "\\linewidth}";
            } else spec += i == 0 ? (selected ? "p{0.43\\linewidth}" : "l") : "r";
        }
        if (selected || !widths.empty()) out += "\\setlength{\\tabcolsep}{3pt}\n";
        out += "\\begin{tabular}{" + spec + "}\n\\toprule\n";
        for (std::size_t i = 0; i < t.columns.size(); ++i) out += (i ? " & " : "") + latex(t.columns[i]);
        out += " \\\\\n\\midrule\n";
        for (const auto& row : t.rows) {
            for (std::size_t i = 0; i < row.size(); ++i) out += (i ? " & " : "") + latex(row[i]);
            out += " \\\\\n";
        }
        out += "\\bottomrule\n\\end{tabular}\n";
    }
    out += "\\end{table}\n";
    return out;
}

// -- pages ---------------------------------------------------------------------------------------

// The marker of a page: HTML comments in Markdown, `%` comments in LaTeX.
struct Markers {
    bool tex = false;
    std::optional<std::string> argument(const std::string& line, std::string_view kind) const {
        const std::string t = trim(line);
        if (tex) {
            const std::string open = "% " + std::string(kind) + ":";
            if (!t.starts_with(open)) return std::nullopt;
            return trim(t.substr(open.size()));
        }
        const std::string open = "<!-- " + std::string(kind) + ":";
        if (!t.starts_with(open) || !t.ends_with("-->")) return std::nullopt;
        return trim(t.substr(open.size(), t.size() - open.size() - 3));
    }
    std::string close(std::string_view kind) const {
        return tex ? "% /" + std::string(kind) : "<!-- /" + std::string(kind) + " -->";
    }
};

std::string sync_page(const fs::path& root, const fs::path& page) {
    const Markers markers{page.extension() == ".tex"};
    const auto lines = lines_of(read_file(page));
    std::string out;
    for (std::size_t i = 0; i < lines.size(); ++i) {
        const std::string& line = lines[i];
        std::optional<std::string> snippet = markers.argument(line, "snippet");
        std::optional<std::string> results = markers.argument(line, "results");
        if (!snippet && !results) {
            out += line + "\n";
            continue;
        }
        const std::string close = markers.close(snippet ? "snippet" : "results");
        std::size_t end = i + 1;
        while (end < lines.size() && trim(lines[end]) != close) ++end;
        if (end == lines.size()) throw Failure{page.string() + ": " + trim(line) + " has no " + close};
        try {
            std::string rendered;
            if (markers.tex) {
                rendered = snippet ? render_snippet_tex(root, *snippet) : render_results_tex(root, *results);
            } else {
                rendered = snippet ? render_snippet(root, *snippet) : render_results(root, *results);
            }
            out += line + "\n" + rendered + close + "\n";
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
    std::vector<std::string> only;
    for (int i = 1; i < argc; ++i) {
        const std::string_view a = argv[i];
        if (a == "--check") {
            check = true;
        } else if (a == "--root" && i + 1 < argc) {
            root = argv[++i];
        } else if (a == "--only" && i + 1 < argc) {
            only.emplace_back(argv[++i]);
        } else {
            std::cerr << "usage: paglets-docs-sync [--root DIR] [--check] [--only FILE]...\n";
            return 2;
        }
    }
    try {
        if (!fs::is_directory(root / "docs")) throw Failure{"no docs/ below " + root.string()};
        std::vector<fs::path> pages;
        if (only.empty()) {
            for (const auto& e : fs::recursive_directory_iterator(root / "docs")) {
                if (e.is_regular_file() && e.path().extension() == ".md") pages.push_back(e.path());
            }
            if (fs::is_directory(root / "papers")) {
                for (const auto& e : fs::recursive_directory_iterator(root / "papers")) {
                    if (e.is_regular_file() && e.path().extension() == ".tex" &&
                        e.path().string().find("/build/") == std::string::npos) {
                        pages.push_back(e.path());
                    }
                }
            }
        } else {
            for (const auto& o : only) {
                const fs::path p = fs::path(o).is_absolute() ? fs::path(o) : root / o;
                if (!fs::is_regular_file(p)) throw Failure{"no file " + p.string()};
                pages.push_back(p);
            }
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
