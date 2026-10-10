// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "jobs.hpp"

#include <algorithm>

namespace paglets::services::impl {

void Jobs::post(std::function<void(bool)> job) {
    std::lock_guard lock(mu_);
    if (stopping_) {
        // After stop: the job learns at once.
        job(true);
        return;
    }
    queue_.push_back(std::move(job));
    if (busy_ + static_cast<int>(queue_.size()) > static_cast<int>(workers_.size()) &&
        static_cast<int>(workers_.size()) < threads_) {
        workers_.emplace_back([this] { run(); });
    }
    cv_.notify_one();
}

void Jobs::run() {
    std::unique_lock lock(mu_);
    while (true) {
        cv_.wait(lock, [&] { return stopping_ || !queue_.empty(); });
        if (queue_.empty()) return;
        auto job = std::move(queue_.front());
        queue_.pop_front();
        ++busy_;
        const bool stopping = stopping_;
        lock.unlock();
        job(stopping);
        lock.lock();
        --busy_;
    }
}

void Jobs::stop() {
    std::vector<std::thread> workers;
    {
        std::lock_guard lock(mu_);
        if (stopping_ && workers_.empty()) return;
        stopping_ = true;
        workers.swap(workers_);
    }
    cv_.notify_all();
    for (auto& t : workers) t.join();
    // Jobs nobody ran: they learn that the host stops.
    std::deque<std::function<void(bool)>> left;
    {
        std::lock_guard lock(mu_);
        left.swap(queue_);
    }
    for (auto& job : left) job(true);
}

std::size_t Jobs::pending() const {
    std::lock_guard lock(mu_);
    return queue_.size() + static_cast<std::size_t>(busy_);
}

}  // namespace paglets::services::impl
