// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// A paglet built outside the paglets repository: answers "greet" requests,
// and "digest" requests with the SHA-256 of a text (option SHA256).

#include <paglets/paglet.hpp>
#include <paglets/sha256.hpp>

#include <string>

namespace {

class Greeter : public paglets::Paglet {
public:
    Greeter() {
        router().on<std::string>("greet", [](const std::string& name, paglets::Message& m) {
            m.reply("greetings, " + name + ", from outside the tree");
        });
        router().on<std::string>("digest", [](const std::string& text, paglets::Message& m) {
            const auto* bytes = reinterpret_cast<const std::uint8_t*>(text.data());
            m.reply("sha256:" + paglets::to_hex(paglets::sha256({bytes, text.size()})));
        });
    }
};

}  // namespace

PAGLETS_PAGLET(Greeter)
