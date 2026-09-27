// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Smallest paglet: answers "greet" requests and counts them.

#include <paglets/paglet.hpp>

#include <string>

namespace {

class Hello : public paglets::Paglet {
public:
    Hello() {
        router().on<std::string>("greet", [this](const std::string& name, paglets::Message& m) {
            ++greetings_;
            m.reply("hello, " + name + " (greeting " + std::to_string(greetings_) + ")");
        });
    }

    void on_created(paglets::Started&) override {
        auto info = paglets::self_info();
        paglets::log("hello paglet created as " + (info ? info->id : std::string("?")));
    }

private:
    std::uint32_t greetings_ = 0;
};

}  // namespace

PAGLETS_PAGLET(Hello)
