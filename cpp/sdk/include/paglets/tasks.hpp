// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Long service operations: one framework lifecycle for start, bounded
// progress updates, completion, cancellation and errors. Filesystem/network
// services retain capability authority on their host; pure CPU jobs instead
// use compute::submit. Neither kind holds a paglet handler while waiting.
#pragma once

#include <paglets/paglet.hpp>

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <utility>

namespace paglets::tasks {

struct Options {
    std::int64_t timeout_ms = 0;  // unknown/unlimited; positive: application's overall deadline
    RequestOptions start;  // capabilities to lend/pass when starting
    std::int64_t request_timeout_ms = 30'000;  // each bounded progress request
};

struct Status {
    std::string state = "starting";  // starting, running, completed, cancelled, failed
    std::uint64_t updates = 0;
    std::int32_t error = 0;
    bool done = false;
    std::int64_t completed = 0;  // work units; total == 0 means unknown
    std::int64_t total = 0;
    std::string host;
};

inline void paglets_encode(msgpack::Writer& w, const Status& s) {
    w.write_map_header(7);
    abi::detail::put(w, "state", s.state);
    abi::detail::put(w, "updates", s.updates);
    abi::detail::put(w, "error", s.error);
    abi::detail::put(w, "done", s.done);
    abi::detail::put(w, "completed", s.completed);
    abi::detail::put(w, "total", s.total);
    abi::detail::put(w, "host", s.host);
}

inline bool paglets_decode(msgpack::Reader& r, Status& s) {
    return abi::detail::read_map(r, [&](std::string_view key) {
        if (key == "state") return msgpack::read_value(r, s.state);
        if (key == "updates") return msgpack::read_value(r, s.updates);
        if (key == "error") return msgpack::read_value(r, s.error);
        if (key == "done") return msgpack::read_value(r, s.done);
        if (key == "completed") return msgpack::read_value(r, s.completed);
        if (key == "total") return msgpack::read_value(r, s.total);
        if (key == "host") return msgpack::read_value(r, s.host);
        return r.skip();
    });
}

namespace detail {

struct Control {
    virtual ~Control() = default;
    virtual Result<void> cancel() = 0;
    virtual Result<Status> status() const = 0;
};

struct Callbacks final : Control {
    std::function<Result<void>()> stop;
    std::function<Result<Status>()> inspect;
    Result<void> cancel() override { return stop(); }
    Result<Status> status() const override { return inspect(); }
};

}  // namespace detail

// Common control handle for service tasks and compute jobs. Copies refer to
// the same work. Dropping a handle does not cancel an autonomous operation.
class TaskHandle {
public:
    TaskHandle() = default;
    explicit TaskHandle(std::shared_ptr<detail::Control> control) : control_(std::move(control)) {}
    explicit operator bool() const { return static_cast<bool>(control_); }
    Result<void> cancel() const { return control_ ? control_->cancel() : Result<void>(std::unexpected(abi::bad_handle)); }
    Result<Status> status() const { return control_ ? control_->status() : Result<Status>(std::unexpected(abi::bad_handle)); }
    std::weak_ptr<detail::Control> weak() const { return control_; }

    static TaskHandle callbacks(std::function<Result<void>()> cancel, std::function<Result<Status>()> status) {
        auto c = std::make_shared<detail::Callbacks>();
        c->stop = std::move(cancel);
        c->inspect = std::move(status);
        return TaskHandle(std::move(c));
    }

private:
    std::shared_ptr<detail::Control> control_;
};

namespace detail {

// Wire protocol shared by service tasks: OP_start replies with {id},
// OP_next({id}) replies with an Update whose `finished` field is bool,
// OP_cancel({id}) requests cooperative cancellation. Update codecs belong
// to the service; task IDs are opaque and caller-scoped on the host.
struct Id { std::string id; };
inline void paglets_encode(msgpack::Writer& w, const Id& q) {
    w.write_map_header(1);
    abi::detail::put(w, "id", q.id);
}
inline bool paglets_decode(msgpack::Reader& r, Id& q) {
    return abi::detail::read_map(r, [&](std::string_view key) {
        return key == "id" ? msgpack::read_value(r, q.id) : r.skip();
    });
}

template <class Update>
class ServiceDriver : public Control, public std::enable_shared_from_this<ServiceDriver<Update>> {
public:
    using OnUpdate = std::function<void(const Update&)>;
    using Done = std::function<void(Result<void>)>;

    Result<Status> status() const override { return status_; }
    bool finished() const { return status_.done; }
    // Completion is local and immediate; the service stops at its next
    // cancellation check. A blocked OS call must return first.
    Result<void> cancel() override { finish(std::unexpected(abi::cancelled)); return {}; }

    static std::shared_ptr<ServiceDriver> start(Endpoint service, std::string operation, Bytes request,
                                                OnUpdate update, Done done, Options options) {
        auto s = std::shared_ptr<ServiceDriver>(new ServiceDriver);
        s->service_ = service;
        s->operation_ = std::move(operation);
        s->update_ = std::move(update);
        s->done_ = std::move(done);
        s->options_ = std::move(options);
        s->began_ns_ = now_ns(abi::Clock::monotonic);
        if (s->options_.timeout_ms < -1 || s->options_.request_timeout_ms <= 0) {
            s->finish(std::unexpected(abi::invalid_argument));
            return s;
        }
        s->options_.start.timeout_ms = s->options_.timeout_ms > 0
            ? std::min(s->options_.timeout_ms, s->options_.request_timeout_ms) : s->options_.request_timeout_ms;
        auto sent = service.request_raw(s->operation_ + "_start", std::move(request), [s](Message& m) {
            if (!m.ok()) return s->finish(std::unexpected(m.status()));
            detail::Id started;
            if (!m.decode(started) || started.id.empty()) return s->finish(std::unexpected(abi::malformed));
            s->id_ = std::move(started.id);
            if (s->finished()) return s->release();  // cancelled while starting
            s->status_.state = "running";
            s->next();
        }, s->options_.start);
        if (!sent) s->finish(std::unexpected(sent.error()));
        return s;
    }

private:
    void release() {
        if (id_.empty()) return;
        SendOptions options;
        options.async = true;
        (void)service_.send(operation_ + "_cancel", detail::Id{std::exchange(id_, {})}, options);
    }
    void finish(Result<void> result) {
        if (finished()) return;
        status_.done = true;
        status_.error = result ? 0 : result.error();
        status_.state = result ? "completed" : result.error() == abi::cancelled ? "cancelled" : "failed";
        release();
        update_ = {};
        auto done = std::move(done_);
        if (done) done(result);
    }
    void next() {
        const auto elapsed_ms = (now_ns(abi::Clock::monotonic) - began_ns_) / 1'000'000;
        if (options_.timeout_ms > 0 && elapsed_ms >= options_.timeout_ms) return finish(std::unexpected(abi::timeout));
        RequestOptions options;
        options.timeout_ms = options_.timeout_ms > 0
            ? std::min(options_.request_timeout_ms, options_.timeout_ms - elapsed_ms) : options_.request_timeout_ms;
        auto s = this->shared_from_this();
        auto sent = service_.request(operation_ + "_next", detail::Id{id_}, [s](Message& m) {
            if (s->finished()) return;
            if (!m.ok()) return s->finish(std::unexpected(m.status()));
            Update update;
            if (!m.decode(update)) return s->finish(std::unexpected(abi::malformed));
            s->progress_ = std::move(update);
            ++s->status_.updates;
            if constexpr (requires { s->progress_.completed; }) s->status_.completed = s->progress_.completed;
            if constexpr (requires { s->progress_.total; }) s->status_.total = s->progress_.total;
            auto callback = s->update_;  // permits cancel() from within the callback
            if (callback) callback(s->progress_);
            if (s->finished()) return;
            if (s->progress_.finished) return s->finish({});
            s->next();
        }, options);
        if (!sent) finish(std::unexpected(sent.error()));
    }
    Endpoint service_;
    std::string operation_;
    std::string id_;
    Options options_;
    Status status_;
    Update progress_{};
    OnUpdate update_;
    Done done_;
    std::int64_t began_ns_ = 0;
};

}  // namespace detail

// Each update runs in a fresh handler. Backpressure is automatic: the next
// update is requested only after the previous callback returns.
template <class Update, class Request>
TaskHandle start(Endpoint service, std::string operation, const Request& request,
                                            typename detail::ServiceDriver<Update>::OnUpdate update,
                                            typename detail::ServiceDriver<Update>::Done done, Options options = {}) {
    return TaskHandle(detail::ServiceDriver<Update>::start(service, std::move(operation), encode(request),
        std::move(update), std::move(done), std::move(options)));
}

}  // namespace paglets::tasks
