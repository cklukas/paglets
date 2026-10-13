// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// The mesh client library (WP31): shell commands for tools.

#include "test.hpp"

#include <paglets/client/client.hpp>

namespace pc = paglets::client;

PAGLETS_TEST("client: shell arguments are quoted only when needed") {
    CHECK_EQ(pc::shell_quote("plain-arg_1.2/x:y=z"), std::string("plain-arg_1.2/x:y=z"));
    CHECK_EQ(pc::shell_quote(""), std::string("''"));
    CHECK_EQ(pc::shell_quote("two words"), std::string("'two words'"));
    CHECK_EQ(pc::shell_quote(R"({"by":1})"), std::string(R"('{"by":1}')"));
    CHECK_EQ(pc::shell_quote("it's"), std::string(R"('it'\''s')"));
    CHECK_EQ(pc::shell_quote("$HOME"), std::string("'$HOME'"));
}

PAGLETS_TEST("client: remote commands carry the connection") {
    pc::ShellContext c;
    c.connect = "https://mac.local:7443";
    c.key_file = "keys/me.key";
    c.ledger_dir = "my ledger";
    CHECK_EQ(pc::remote_command(c, "call", {"abc", "increment", R"({"by":2})"}),
             std::string("paglets-host remote call --connect https://mac.local:7443 --key keys/me.key "
                         "--ledger 'my ledger' abc increment '{\"by\":2}'"));
    c.host_key = "00ff";
    CHECK_EQ(pc::remote_command(c, "hosts"),
             std::string("paglets-host remote hosts --connect https://mac.local:7443 --key keys/me.key "
                         "--ledger 'my ledger' --host-key 00ff"));
}
