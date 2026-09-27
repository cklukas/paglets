// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Counter example paglet. Its state is ordinary C++ objects on the heap and
// in static storage; memory images move it without any serialization code.

#include "counter.schema.gen.hpp"

#include <paglets/paglet.hpp>

#include <string>
#include <vector>

namespace {

using namespace counter_msgs;

class Counter : public paglets::Paglet {
public:
    Counter() {
        router().on<Increment>("increment", [this](const Increment& inc, paglets::Message& m) {
            value_ = inc.mode == Mode::add ? value_ + inc.by : value_ * inc.by;
            history_.push_back(inc);
            m.reply(status());
        });
        router().on("status", [this](paglets::Message& m) { m.reply(status()); });
        router().on<Echo>("echo", [](const Echo& echo, paglets::Message& m) { m.reply(echo); });
        router().on<Bloat>("bloat", [this](const Bloat& bloat, paglets::Message& m) {
            ballast_.emplace_back(std::size_t{bloat.kilobytes} * 1024, bloat.fill);
            m.reply(status());
        });
        router().on("schema", [](paglets::Message& m) { m.reply(std::string(paglets_schema_json)); });
        router().on<std::string>("log", [this](const std::string& text, paglets::Message& m) {
            paglets::log(text);
            m.reply(status());
        });
    }

private:
    Status status() const {
        Status s;
        s.value = value_;
        s.history_size = static_cast<std::uint32_t>(history_.size());
        const std::size_t first = history_.size() > 3 ? history_.size() - 3 : 0;
        for (std::size_t i = first; i < history_.size(); ++i) {
            s.recent_notes.push_back(history_[i].note);
        }
        if (!history_.empty()) {
            double sum = 0;
            for (const auto& h : history_) sum += static_cast<double>(h.by);
            s.average_step = sum / static_cast<double>(history_.size());
        }
        s.memory_pages = static_cast<std::uint32_t>(__builtin_wasm_memory_size(0));
        return s;
    }

    std::int64_t value_ = 0;
    std::vector<Increment> history_;
    std::vector<std::vector<std::uint8_t>> ballast_;
};

}  // namespace

PAGLETS_PAGLET(Counter)
