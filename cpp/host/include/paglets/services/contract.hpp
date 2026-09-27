// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Native system paglets that serve a contract (wire/schema.hpp) through
// reflection: the message name selects the operation, the payload decodes
// into its request type, and the implementation's member function of the
// same name returns the reply or an abi::Error.
//
//     struct Calc : paglets::services::ContractPaglet<calc::Contract, Calc> {
//         paglets::services::Result<calc::AddReply> add(const calc::AddRequest& q, Operation& op) {
//             return calc::AddReply{q.a + q.b};
//         }
//     };
//
// `describe` answers the schema descriptor of the contract's namespace,
// identical to the one the guest schema generator embeds.

#pragma once

#include <paglets/abi.hpp>
#include <paglets/runtime/system.hpp>
#include <paglets/wire/reflect.hpp>
#include <paglets/wire/schema.hpp>

#include <cstdint>
#include <expected>
#include <meta>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::services {

template <class T>
using Result = std::expected<T, std::int32_t>;

// What an operation sees besides its request.
struct Operation {
    runtime::SystemContext& ctx;
    runtime::ServiceCall& call;
    // Capabilities to return with the reply.
    std::vector<runtime::Cap> give;

    const std::optional<abi::SenderRecord>& sender() const { return call.sender(); }
    const std::vector<runtime::Cap>& lent() const { return call.lent(); }
};

namespace detail {

// The member function of `impl` named `name` (a compile-time error names
// the missing operation).
consteval std::meta::info implementation_of(std::meta::info impl, std::string_view name) {
    using namespace std::meta;
    for (auto m : members_of(impl, access_context::unchecked())) {
        if (is_function(m) && has_identifier(m) && identifier_of(m) == name) return m;
    }
    throw "the implementation lacks an operation of its contract";
}

}  // namespace detail

template <class Contract, class Impl>
class ContractPaglet : public runtime::SystemPaglet {
public:
    std::string_view name() const override { return Contract::service; }

    std::vector<std::string> operations() const override {
        std::vector<std::string> ops;
        template for (constexpr auto op : std::define_static_array(wire::operations_of(^^Contract))) {
            ops.emplace_back(std::meta::identifier_of(op));
        }
        ops.emplace_back("describe");
        return ops;
    }

    // The descriptor of the contract's schema namespace (JSON).
    static std::string describe() { return wire::namespace_descriptor<std::meta::parent_of(^^Contract)>().dump(); }

    void handle(runtime::SystemContext& ctx, runtime::ServiceCall& call) final {
        bool found = false;
        template for (constexpr auto op : std::define_static_array(wire::operations_of(^^Contract))) {
            if (!found && call.name() == std::meta::identifier_of(op)) {
                found = true;
                run<op>(ctx, call);
            }
        }
        if (found) return;
        if (call.name() == "describe") {
            (void)call.reply(0, wire::to_msgpack(describe()));
        } else if (call.is_request()) {
            (void)call.reply(abi::unknown_message);
        }
    }

private:
    template <std::meta::info Op>
    void run(runtime::SystemContext& ctx, runtime::ServiceCall& call) {
        using Request = [:wire::request_type(Op):];
        using Reply = [:wire::reply_type(Op):];
        Request request{};
        if (!wire::from_msgpack(call.payload(), request)) {
            if (call.is_request()) (void)call.reply(abi::malformed);
            return;
        }
        Operation op{ctx, call, {}};
        constexpr auto fn = detail::implementation_of(^^Impl, std::meta::identifier_of(Op));
        Result<Reply> result = static_cast<Impl&>(*this).[:fn:](request, op);
        // Messages (not requests) have nobody to answer; an operation may
        // also have answered or deferred the request itself.
        if (!call.is_request() || call.replied()) return;
        if (result) {
            (void)call.reply(0, wire::to_msgpack(*result), std::move(op.give));
        } else {
            (void)call.reply(result.error());
        }
    }
};

}  // namespace paglets::services
