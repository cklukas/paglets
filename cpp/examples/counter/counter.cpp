// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Counter example paglet. Its state is ordinary C++ objects on the heap and
// in static storage; memory images move it without any serialization code.

#include "counter.schema.gen.hpp"

#include <paglets/guest.hpp>

#include <string>
#include <vector>

namespace {

std::int64_t value = 0;
std::vector<counter_msgs::Increment> history;
std::vector<std::vector<std::uint8_t>> ballast;

std::uint32_t memory_pages() {
    return static_cast<std::uint32_t>(__builtin_wasm_memory_size(0));
}

counter_msgs::Status status() {
    counter_msgs::Status s;
    s.value = value;
    s.history_size = static_cast<std::uint32_t>(history.size());
    const std::size_t first = history.size() > 3 ? history.size() - 3 : 0;
    for (std::size_t i = first; i < history.size(); ++i) {
        s.recent_notes.push_back(history[i].note);
    }
    if (!history.empty()) {
        double sum = 0;
        for (const auto& h : history) sum += static_cast<double>(h.by);
        s.average_step = sum / static_cast<double>(history.size());
    }
    s.memory_pages = memory_pages();
    return s;
}

template <class T>
bool read_body(paglets::msgpack::Reader& r, T& body) {
    return paglets::msgpack::read_value(r, body) && r.ok();
}

}  // namespace

std::vector<std::uint8_t> paglets::guest::handle_message(std::span<const std::uint8_t> request) {
    paglets::msgpack::Reader r(request);
    std::uint32_t parts = 0;
    std::string_view name;
    if (!r.read_array_header(parts) || parts != 2 || !r.read_str(name)) {
        return reply_error("malformed request");
    }

    if (name == "increment") {
        counter_msgs::Increment inc;
        if (!read_body(r, inc)) return reply_error("malformed increment");
        value = inc.mode == counter_msgs::Mode::add ? value + inc.by : value * inc.by;
        history.push_back(std::move(inc));
        return reply_ok(status());
    }
    if (name == "status") {
        return reply_ok(status());
    }
    if (name == "echo") {
        counter_msgs::Echo echo;
        if (!read_body(r, echo)) return reply_error("malformed echo");
        return reply_ok(echo);
    }
    if (name == "bloat") {
        counter_msgs::Bloat bloat;
        if (!read_body(r, bloat)) return reply_error("malformed bloat");
        ballast.emplace_back(std::size_t{bloat.kilobytes} * 1024, bloat.fill);
        return reply_ok(status());
    }
    if (name == "schema") {
        return reply_ok(std::string(counter_msgs::paglets_schema_json));
    }
    if (name == "log") {
        std::string text;
        if (!read_body(r, text)) return reply_error("malformed log");
        paglets::guest::log(text);
        return reply_ok(status());
    }
    return reply_error("unknown message: " + std::string(name));
}
