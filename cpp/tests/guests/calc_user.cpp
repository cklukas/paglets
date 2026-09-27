// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Serves the calc contract with code generated from calc_contract.hpp, and
// calls the host's calc service through the generated client.

#include "calc_user.schema.gen.hpp"

#include <paglets/paglet.hpp>

#include <string>

namespace {

using paglets::Message;

class CalcUser : public paglets::Paglet {
public:
    CalcUser() {
        calc::serve(router(), *this);
        // Asks the host's calc service and answers with its result, or with
        // "error:<status>".
        router().on<calc::DivRequest>("use_service", [](const calc::DivRequest& q, Message& m) -> std::int32_t {
            auto endpoint = paglets::service("calc");
            if (!endpoint) return endpoint.error();
            paglets::Capability pending = m.defer_reply();
            calc::Client client{*endpoint};
            auto sent = client.divide(q, [pending](paglets::Result<calc::DivReply> r, Message&) mutable {
                if (r) {
                    paglets::reply_to(pending, *r);
                } else {
                    paglets::reply_to(pending, "error:" + std::string(paglets::abi::error_name(r.error())));
                }
            });
            if (!sent) return sent.error();
            return paglets::abi::handled;
        });
    }

    paglets::Result<calc::AddReply> add(const calc::AddRequest& q, Message&) { return calc::AddReply{q.a + q.b}; }

    paglets::Result<calc::DivReply> divide(const calc::DivRequest& q, Message&) {
        if (q.b == 0) return std::unexpected(paglets::abi::invalid_argument);
        return calc::DivReply{q.a / q.b, "guest"};
    }
};

}  // namespace

PAGLETS_PAGLET(CalcUser)
