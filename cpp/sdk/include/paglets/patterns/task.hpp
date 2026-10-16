// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Patterns (WP18): a paglet that runs one task and reports on it.
//
//     struct Count : paglets::patterns::Task<CountRequest, CountResult> {
//         void run(const CountRequest& q) override {
//             ...  // possibly over several handlers and moves
//             complete(CountResult{n});  // or fail("why")
//         }
//     };
//
// Messages: `start` (the request; answered with the status once run()
// returns),
// `status`, `cancel`, and `wait` (answered when the task is done, or after
// `timeout_ms` with the status as it is). The result travels encoded in the
// status, so clients decode it with their own type.

#pragma once

#include <paglets/paglet.hpp>
#include <paglets/tasks.hpp>

#include <algorithm>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace paglets::patterns {

struct TaskStatus {
    std::string state = "idle";  // idle, running, completed, failed, cancelled
    bool done = false;
    std::string error;
    std::int64_t started_ms = 0;  // Unix milliseconds
    std::int64_t completed_ms = 0;
    Bytes result;  // completed: the result, encoded
    std::int64_t completed = 0;  // progress; total == 0 means unknown
    std::int64_t total = 0;

    template <class T>
    bool decode_result(T& out) const {
        return paglets::decode(result, out);
    }
};

struct WaitRequest {
    std::int64_t timeout_ms = 30'000;
};

inline void paglets_encode(msgpack::Writer& w, const TaskStatus& s) {
    w.write_map_header(8);
    abi::detail::put(w, "state", s.state);
    abi::detail::put(w, "done", s.done);
    abi::detail::put(w, "error", s.error);
    abi::detail::put(w, "started_ms", s.started_ms);
    abi::detail::put(w, "completed_ms", s.completed_ms);
    abi::detail::put(w, "result", s.result);
    abi::detail::put(w, "completed", s.completed);
    abi::detail::put(w, "total", s.total);
}

inline bool paglets_decode(msgpack::Reader& r, TaskStatus& s) {
    return abi::detail::read_map(r, [&](std::string_view k) {
        if (k == "state") return msgpack::read_value(r, s.state);
        if (k == "done") return msgpack::read_value(r, s.done);
        if (k == "error") return msgpack::read_value(r, s.error);
        if (k == "started_ms") return msgpack::read_value(r, s.started_ms);
        if (k == "completed_ms") return msgpack::read_value(r, s.completed_ms);
        if (k == "result") return msgpack::read_value(r, s.result);
        if (k == "completed") return msgpack::read_value(r, s.completed);
        if (k == "total") return msgpack::read_value(r, s.total);
        return r.skip();
    });
}

inline void paglets_encode(msgpack::Writer& w, const WaitRequest& q) {
    w.write_map_header(1);
    abi::detail::put(w, "timeout_ms", q.timeout_ms);
}

inline bool paglets_decode(msgpack::Reader& r, WaitRequest& q) {
    return abi::detail::read_map(r, [&](std::string_view k) {
        if (k == "timeout_ms") return msgpack::read_value(r, q.timeout_ms);
        return r.skip();
    });
}

inline std::int64_t unix_ms() {
    return now_ns(abi::Clock::wall) / 1'000'000;
}

template <class Request, class TaskResult>
class Task : public Paglet {
public:
    Task() {
        router().template on<Request>("start", [this](const Request& q, Message& m) {
            prune_children();
            if (status_.state == "running" || !children_.empty()) {
                (void)m.reply(status_);
                return;
            }
            status_ = TaskStatus{};
            status_.state = "running";
            status_.started_ms = unix_ms();
            run(q);  // may complete or fail at once
            (void)m.reply(status_);
        });
        router().on("status", [this](Message& m) { (void)m.reply(status_); });
        router().on("cancel", [this](Message& m) { cancel(); (void)m.reply(status_); });
        router().on("wait", [this](Message& m) {
            WaitRequest q;
            if (!m.payload().empty() && !m.decode(q)) {
                (void)m.reply(status_);
                return;
            }
            if (status_.done || !m.is_request()) {
                (void)m.reply(status_);
                return;
            }
            const std::int64_t timeout = std::clamp<std::int64_t>(q.timeout_ms, 0, std::numeric_limits<std::int64_t>::max() - unix_ms());
            auto timer = after_raw(timeout, "task.wait-timeout", {});
            if (!timer) { (void)m.reply(status_); return; }
            waiters_.push_back(Waiter{m.defer_reply(), unix_ms() + timeout, *timer});
        });
        router().on("task.wait-timeout", [this](Message&) { answer_waiters(false); });
    }

    const TaskStatus& task_status() const { return status_; }

    // Task is the paglet-level task abstraction; TaskHandle is its control
    // handle, also used for service tasks and compute jobs.
    tasks::TaskHandle handle() {
        return tasks::TaskHandle::callbacks([this]() -> Result<void> { cancel(); return {}; },
            [this]() -> Result<tasks::Status> {
                tasks::Status s;
                s.state = status_.state;
                s.done = status_.done;
                s.completed = status_.completed;
                s.total = status_.total;
                s.error = s.state == "cancelled" ? abi::cancelled : s.state == "failed" ? abi::failed : 0;
                return s;
            });
    }

    void cancel() {
        if (status_.state != "running") return;
        status_.state = "cancelled";
        status_.done = true;
        status_.error = "cancelled";
        status_.completed_ms = unix_ms();
        auto children = children_;
        for (const auto& child : children) (void)child.cancel();
        on_cancel();
        answer_waiters(true);
    }

protected:
    // Starts the task; complete() or fail() end it (now or later).
    virtual void run(const Request& request) = 0;
    virtual void on_cancel() {}

    // Keep child work under this task's lifecycle. Cancelling the Task
    // cancels all tracked work, regardless of its execution backend.
    tasks::TaskHandle track(tasks::TaskHandle child) {
        if (status_.state != "running") { (void)child.cancel(); return child; }
        prune_children();
        children_.push_back(child);
        return child;
    }

    void progress(std::int64_t completed, std::int64_t total = 0) {
        if (status_.state != "running") return;
        status_.completed = completed;
        status_.total = total;
    }

    void complete(const TaskResult& result) {
        if (status_.state != "running") return;
        status_.state = "completed";
        status_.done = true;
        status_.error.clear();
        status_.result = encode(result);
        status_.completed_ms = unix_ms();
        answer_waiters(true);
        auto children = children_;
        for (const auto& child : children) (void)child.cancel();
    }

    void fail(std::string error) {
        if (status_.state != "running") return;
        status_.state = "failed";
        status_.done = true;
        status_.error = std::move(error);
        status_.completed_ms = unix_ms();
        answer_waiters(true);
        auto children = children_;
        for (const auto& child : children) (void)child.cancel();
    }

private:
    // Do not restart while a cancelled child is still delivering its final
    // continuation: that continuation belongs to the previous run.
    void prune_children() {
        std::erase_if(children_, [](const auto& child) { auto s = child.status(); return !s || s->done; });
    }
    struct Waiter {
        Capability reply;
        std::int64_t deadline = 0;
        Capability timer;
    };

    // Answers every waiter (all: the task is done) or those whose time is up.
    void answer_waiters(bool all) {
        const std::int64_t now = unix_ms();
        for (auto it = waiters_.begin(); it != waiters_.end();) {
            if (all || it->deadline <= now) {
                (void)reply_to(it->reply, status_);
                (void)it->timer.drop();
                it = waiters_.erase(it);
            } else {
                ++it;
            }
        }
    }

    TaskStatus status_;
    std::vector<Waiter> waiters_;
    std::vector<tasks::TaskHandle> children_;
};

}  // namespace paglets::patterns
