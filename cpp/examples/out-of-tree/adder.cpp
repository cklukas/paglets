// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Serves the adder contract (adder_contract.hpp) with the generated
// adder::serve(), and asks the server-info system service through its
// generated client (found with the patterns library).

#include "adder.schema.gen.hpp"

#include <paglets/paglet.hpp>
#include <paglets/patterns.hpp>
#include <paglets/services/server_info.gen.hpp>

#include <string>

namespace {

namespace si = paglets::services::server_info;
using paglets::Message;

class Adder : public paglets::Paglet {
public:
    Adder() { adder::serve(router(), *this); }

    paglets::Result<adder::AddReply> add(const adder::AddRequest& q, Message&) { return adder::AddReply{q.a + q.b}; }

    // Replies later, when server-info has answered.
    paglets::Result<adder::HostReply> host(const adder::HostRequest&, Message& m) {
        paglets::Capability pending = m.defer_reply();
        paglets::patterns::endpoint("server-info", [pending](paglets::Result<paglets::Endpoint> endpoint) mutable {
            if (!endpoint) return fail(pending, endpoint.error());
            si::Client client{*endpoint};
            auto sent = client.summary({}, [pending](paglets::Result<si::Summary> r, Message&) mutable {
                if (!r) return fail(pending, r.error());
                paglets::reply_to(pending, adder::HostReply{r->os});
            });
            if (!sent) fail(pending, sent.error());
        });
        return adder::HostReply{};  // not sent: the reply is deferred
    }

private:
    static void fail(paglets::Capability& pending, std::int32_t status) {
        paglets::reply_to(pending, "error:" + std::string(paglets::abi::error_name(status)));
    }
};

}  // namespace

PAGLETS_PAGLET(Adder)
