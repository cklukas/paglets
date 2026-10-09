// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// A clone bomb, for the hardening tests: every instance clones itself four
// times when it starts, so the instances double and more with each
// generation until the host refuses (quota). `refused` answers how many of
// its clones were refused.

#include <paglets/paglet.hpp>

#include <cstdint>

class Bomb : public paglets::Paglet {
public:
    Bomb() {
        router().on("refused", [this](paglets::Message& m) { (void)m.reply(refused_); });
    }

    void on_created(paglets::Started&) override { burst(); }
    void on_cloned(paglets::Started&) override { burst(); }

private:
    void burst() {
        for (int i = 0; i < 4; ++i) {
            auto clone = paglets::clone_raw();
            if (!clone) {
                ++refused_;
            } else {
                (void)clone->drop();
            }
        }
    }

    std::int64_t refused_ = 0;
};

PAGLETS_PAGLET(Bomb)
