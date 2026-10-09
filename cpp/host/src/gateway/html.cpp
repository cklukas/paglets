// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Readable text of HTML pages for `web.extract_text`: no DOM, a single pass
// that drops scripts, styles and markup, keeps block structure as line
// breaks, decodes common entities, and collects links.

#include "gateway_impl.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>

namespace paglets::gateway {

namespace {

std::string lower(std::string_view s) {
    std::string out(s);
    std::ranges::transform(out, out.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return out;
}

void append_utf8(std::string& out, std::uint32_t cp) {
    if (cp == 0 || cp > 0x10ffff || (cp >= 0xd800 && cp <= 0xdfff)) cp = 0xfffd;
    if (cp < 0x80) {
        out += static_cast<char>(cp);
    } else if (cp < 0x800) {
        out += static_cast<char>(0xc0 | (cp >> 6));
        out += static_cast<char>(0x80 | (cp & 0x3f));
    } else if (cp < 0x10000) {
        out += static_cast<char>(0xe0 | (cp >> 12));
        out += static_cast<char>(0x80 | ((cp >> 6) & 0x3f));
        out += static_cast<char>(0x80 | (cp & 0x3f));
    } else {
        out += static_cast<char>(0xf0 | (cp >> 18));
        out += static_cast<char>(0x80 | ((cp >> 12) & 0x3f));
        out += static_cast<char>(0x80 | ((cp >> 6) & 0x3f));
        out += static_cast<char>(0x80 | (cp & 0x3f));
    }
}

std::string decode_entities(std::string_view s) {
    std::string out;
    out.reserve(s.size());
    for (std::size_t i = 0; i < s.size(); ++i) {
        if (s[i] != '&') {
            out += s[i];
            continue;
        }
        const auto semi = s.find(';', i);
        if (semi == std::string_view::npos || semi - i > 10) {
            out += '&';
            continue;
        }
        const std::string_view name = s.substr(i + 1, semi - i - 1);
        std::uint32_t cp = 0;
        if (name.starts_with('#')) {
            const bool hex = name.size() > 1 && (name[1] == 'x' || name[1] == 'X');
            const std::string_view digits = name.substr(hex ? 2 : 1);
            auto [p, ec] = std::from_chars(digits.data(), digits.data() + digits.size(), cp, hex ? 16 : 10);
            if (ec != std::errc() || p != digits.data() + digits.size()) cp = 0xfffd;
        } else if (name == "amp") {
            cp = '&';
        } else if (name == "lt") {
            cp = '<';
        } else if (name == "gt") {
            cp = '>';
        } else if (name == "quot") {
            cp = '"';
        } else if (name == "apos") {
            cp = '\'';
        } else if (name == "nbsp") {
            cp = ' ';
        } else if (name == "copy") {
            cp = 0xa9;
        } else if (name == "mdash") {
            cp = 0x2014;
        } else if (name == "ndash") {
            cp = 0x2013;
        } else if (name == "hellip") {
            cp = 0x2026;
        } else {
            out += '&';
            continue;
        }
        append_utf8(out, cp);
        i = semi;
    }
    return out;
}

// Collapses runs of spaces; keeps single line breaks between blocks.
std::string tidy(std::string_view s) {
    std::string out;
    bool space = false;
    int breaks = 0;
    for (char c : s) {
        if (c == '\n') {
            ++breaks;
            space = false;
            continue;
        }
        if (c == ' ' || c == '\t' || c == '\r' || c == '\f' || c == '\v') {
            space = true;
            continue;
        }
        if (!out.empty()) {
            if (breaks > 0) {
                out += breaks > 1 ? "\n\n" : "\n";
            } else if (space) {
                out += ' ';
            }
        }
        breaks = 0;
        space = false;
        out += c;
    }
    return out;
}

std::string attribute(std::string_view tag, std::string_view name) {
    const std::string low = lower(tag);
    std::size_t at = 0;
    while ((at = low.find(name, at)) != std::string::npos) {
        const bool start = at > 0 && (low[at - 1] == ' ' || low[at - 1] == '\t' || low[at - 1] == '\n');
        std::size_t i = at + name.size();
        while (i < low.size() && low[i] == ' ') ++i;
        if (!start || i >= low.size() || low[i] != '=') {
            at += name.size();
            continue;
        }
        ++i;
        while (i < low.size() && low[i] == ' ') ++i;
        if (i >= tag.size()) return {};
        if (tag[i] == '"' || tag[i] == '\'') {
            const char q = tag[i];
            const auto end = tag.find(q, i + 1);
            return std::string(tag.substr(i + 1, end == std::string_view::npos ? std::string_view::npos : end - i - 1));
        }
        const auto end = tag.find_first_of(" \t\n>", i);
        return std::string(tag.substr(i, end == std::string_view::npos ? std::string_view::npos : end - i));
    }
    return {};
}

bool block_tag(std::string_view name) {
    static constexpr std::string_view blocks[] = {
        "p",  "div", "br", "li", "ul", "ol", "tr", "table", "h1", "h2", "h3", "h4", "h5", "h6", "section",
        "article", "header", "footer", "nav", "main", "aside", "blockquote", "pre", "hr", "dd", "dt", "td", "th"};
    return std::ranges::find(blocks, name) != std::end(blocks);
}

}  // namespace

Extracted extract_html(std::string_view html, std::string_view base_url) {
    Extracted out;
    std::string text;
    std::string link_href;
    std::string link_text;
    bool in_link = false;
    bool in_title = false;
    std::string title;
    std::size_t i = 0;
    while (i < html.size()) {
        if (html[i] != '<') {
            const auto next = html.find('<', i);
            const std::string_view chunk = html.substr(i, next == std::string_view::npos ? std::string_view::npos : next - i);
            if (in_title) {
                title += chunk;
            } else {
                text += chunk;
                if (in_link) link_text += chunk;
            }
            i = next == std::string_view::npos ? html.size() : next;
            continue;
        }
        if (html.substr(i, 4) == "<!--") {
            const auto end = html.find("-->", i + 4);
            i = end == std::string_view::npos ? html.size() : end + 3;
            continue;
        }
        const auto close = html.find('>', i);
        if (close == std::string_view::npos) break;
        const std::string_view tag = html.substr(i + 1, close - i - 1);
        i = close + 1;
        const bool closing = tag.starts_with('/');
        std::string_view name_part = closing ? tag.substr(1) : tag;
        const auto name_end = name_part.find_first_of(" \t\n/");
        const std::string name = lower(name_part.substr(0, name_end));
        if (!closing && (name == "script" || name == "style" || name == "noscript" || name == "template")) {
            // Skips everything up to the matching end tag.
            const std::string low = lower(html.substr(i));
            const auto end = low.find("</" + name);
            if (end == std::string::npos) break;
            i += end;
            const auto gt = html.find('>', i);
            i = gt == std::string_view::npos ? html.size() : gt + 1;
            continue;
        }
        if (name == "title") {
            in_title = !closing;
            continue;
        }
        if (name == "a") {
            if (!closing) {
                in_link = true;
                link_href = attribute(tag, "href");
                link_text.clear();
            } else if (in_link) {
                in_link = false;
                std::string url = resolve_url(base_url, decode_entities(link_href));
                if (!url.empty() && out.links.size() < 2000) {
                    std::string t = tidy(decode_entities(link_text));
                    std::ranges::replace(t, '\n', ' ');
                    out.links.emplace_back(std::move(url), std::move(t));
                }
            }
            continue;
        }
        if (block_tag(name)) text += '\n';
        if (name == "p" || name == "h1" || name == "h2" || name == "h3" || name == "tr") text += '\n';
    }
    out.title = tidy(decode_entities(title));
    std::ranges::replace(out.title, '\n', ' ');
    out.text = tidy(decode_entities(text));
    return out;
}

}  // namespace paglets::gateway
