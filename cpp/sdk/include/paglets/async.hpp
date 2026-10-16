// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Coroutines for paglets: `co_await` a request or a compute job instead of
// writing a continuation.
//
//     paglets::Async Pi::compute(Chunk c) {
//         auto r = co_await paglets::compute::run("digits", c, {.hosts = {"any"}});
//         if (!r) co_return;  // r.error(): cancelled, timeout, ...
//         write(r->data);
//     }
//
// An Async starts at once and runs until its first co_await; the handler that
// started it returns, and the coroutine continues in the handler that
// receives the reply. Coroutine frames live on the paglet's heap, in linear
// memory, so a paglet waiting in co_await is an ordinary memory image: it
// can deactivate, be checkpointed and move like any paglet waiting for a
// reply. (A frame is part of the module's code layout: an image resumes only
// with the same module, which is always the case.) A host restart answers
// what was in flight with `gone` or `timeout`, so the coroutine continues
// then, too.

#pragma once

#include <paglets/compute.hpp>
#include <paglets/paglet.hpp>

#include <coroutine>
#include <cstdlib>
#include <optional>
#include <utility>

namespace paglets {

// A coroutine started by a handler; nothing waits for it.
struct Async {
    struct promise_type {
        Async get_return_object() noexcept { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() noexcept { std::abort(); }
    };
};

// The reply of a request, for co_await.
struct Reply {
    std::int32_t status = 0;  // 0, or an abi::Error
    Bytes payload;
    std::vector<Capability> caps;

    bool ok() const { return status == 0; }
    template <class T>
    bool decode(T& out) const {
        return paglets::decode(payload, out);
    }
};

namespace detail {

// Common part of the awaitables: submits in await_suspend, resumes the
// coroutine from the continuation with the result.
template <class Value>
struct Awaiting {
    std::optional<Value> value;
    std::coroutine_handle<> handle;

    bool await_ready() const noexcept { return value.has_value(); }
    Value await_resume() { return std::move(*value); }
};

}  // namespace detail

// co_await ask(endpoint, "name", body): sends a request, gives its reply.
class Ask : public detail::Awaiting<Reply> {
public:
    Ask(Endpoint to, std::string name, Bytes payload, RequestOptions options)
        : to_(to), name_(std::move(name)), payload_(std::move(payload)), options_(std::move(options)) {}

    bool await_suspend(std::coroutine_handle<> h) {
        handle = h;
        auto r = to_.request_raw(name_, std::move(payload_), [this](Message& m) {
            Reply out{m.status(), Bytes(m.payload().begin(), m.payload().end()), {}};
            for (std::size_t i = 0; i < m.cap_count(); ++i) out.caps.push_back(m.take_cap(i));
            value = std::move(out);
            handle.resume();
        }, std::move(options_));
        if (!r) {
            value = Reply{r.error(), {}, {}};
            return false;  // continue at once
        }
        detail::abandon_on_clone(*r, [h] { h.destroy(); });
        return true;
    }

private:
    Endpoint to_;
    std::string name_;
    Bytes payload_;
    RequestOptions options_;
};

inline Ask ask_raw(Endpoint to, std::string name, Bytes payload = {}, RequestOptions options = {}) {
    return Ask(to, std::move(name), std::move(payload), std::move(options));
}
template <class T>
Ask ask(Endpoint to, std::string name, const T& body, RequestOptions options = {}) {
    return Ask(to, std::move(name), paglets::encode(body), std::move(options));
}

namespace compute {

// co_await run("function", input): submits a job, gives its result.
class Run : public paglets::detail::Awaiting<Result<Output>> {
public:
    Run(std::string function, Bytes input, Options options, Job* job)
        : function_(std::move(function)), input_(std::move(input)), options_(std::move(options)), job_(job) {}

    bool await_suspend(std::coroutine_handle<> h) {
        handle = h;
        auto j = submit_raw(function_, std::move(input_), [this](Result<Output> r, Message&) {
            value = std::move(r);
            handle.resume();
        }, std::move(options_));
        if (!j) {
            value = std::unexpected(j.error());
            return false;
        }
        if (job_ != nullptr) *job_ = *j;
        paglets::detail::abandon_on_clone(j->id(), [h] { h.destroy(); });
        return true;
    }

private:
    std::string function_;
    Bytes input_;
    Options options_;
    Job* job_;
};

// `job`, if given, receives the job while it runs (to cancel it, or ask its status).
inline Run run_raw(std::string function, Bytes input, Options options = {}, Job* job = nullptr) {
    return Run(std::move(function), std::move(input), std::move(options), job);
}
template <class T>
Run run(std::string function, const T& input, Options options = {}, Job* job = nullptr) {
    return Run(std::move(function), paglets::encode(input), std::move(options), job);
}

}  // namespace compute
}  // namespace paglets
