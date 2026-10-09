// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Patterns (WP18): typed operations. serve() registers a handler that
// takes the request type and returns the reply (or an abi error), and
// answers the request itself:
//
//     patterns::serve<AddRequest, AddReply>(router(), "add",
//         [](const AddRequest& q) -> Result<AddReply> { return AddReply{q.a + q.b}; });
//
// call() is the client side: the reply decodes into the reply type.

#pragma once

#include <paglets/paglet.hpp>

#include <functional>
#include <string>
#include <utility>

namespace paglets::patterns {

template <class Request, class Reply, class F>
void serve(Router& router, std::string name, F&& f) {
    router.on<Request>(std::move(name), [g = std::forward<F>(f)](const Request& q, Message& m) -> std::int32_t {
        Result<Reply> r = g(q);
        if (!r) return r.error();
        if (m.is_request()) (void)m.reply(*r);
        return abi::handled;
    });
}

template <class Reply, class Request>
Result<std::uint64_t> call(const Endpoint& to, std::string_view op, const Request& q,
                           std::function<void(Result<Reply>)> next, RequestOptions options = {}) {
    return to.request(
        op, q,
        [next](Message& reply) {
            if (!reply.ok()) return next(std::unexpected(reply.status()));
            Reply r{};
            if (!reply.decode(r)) return next(std::unexpected(abi::malformed));
            next(std::move(r));
        },
        std::move(options));
}

}  // namespace paglets::patterns
