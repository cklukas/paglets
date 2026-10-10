// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// A small pool of threads for system paglet work that blocks (network,
// inference, copying and hashing large files), so it does not hold a
// scheduler lane.

#pragma once

#include <algorithm>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace paglets::services::impl {

// At most `threads` jobs at a time, the rest wait. stop() ends the threads
// after the jobs that run; waiting jobs run with `stopping` set.
class Jobs {
public:
    explicit Jobs(int threads) : threads_(std::max(1, threads)) {}
    ~Jobs() { stop(); }
    void post(std::function<void(bool stopping)> job);
    void stop();
    std::size_t pending() const;

private:
    void run();

    int threads_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::deque<std::function<void(bool)>> queue_;
    std::vector<std::thread> workers_;
    int busy_ = 0;
    bool stopping_ = false;
};

}  // namespace paglets::services::impl
