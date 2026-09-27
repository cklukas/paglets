// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "services.hpp"

#include <algorithm>

namespace paglets::services::impl {

void ServerInfo::start(runtime::SystemContext&) {
    // The baseline of the first `load`.
    std::lock_guard lock(mu_);
    last_ = platform::cpu_sample();
}

Result<server_info::Summary> ServerInfo::summary(const server_info::SummaryRequest&, Operation&) {
    const auto s = platform::summary();
    return server_info::Summary{s.os,        s.os_version,   s.host_name, s.architecture, s.cpu_model,
                                s.cpu_count, s.memory_total, s.uptime_s,  now_ms()};
}

Result<server_info::Load> ServerInfo::load(const server_info::LoadRequest&, Operation&) {
    const auto sample = platform::cpu_sample();
    server_info::Load load;
    {
        std::lock_guard lock(mu_);
        load.cpu_percent = platform::cpu_percent(last_.all, sample.all);
        for (std::size_t i = 0; i < sample.cores.size(); ++i) {
            load.per_core.push_back(i < last_.cores.size() ? platform::cpu_percent(last_.cores[i], sample.cores[i])
                                                           : 0.0);
        }
        last_ = sample;
    }
    load.load_average = platform::load_average();
    const auto memory = platform::memory();
    load.memory_total = memory.total;
    load.memory_available = memory.available;
    load.memory_used = memory.total > memory.available ? memory.total - memory.available : 0;
    return load;
}

Result<server_info::Volumes> ServerInfo::volumes(const server_info::VolumesRequest&, Operation&) {
    server_info::Volumes out;
    for (auto& v : platform::volumes()) {
        out.volumes.push_back(server_info::Volume{std::move(v.name), std::move(v.mount_point), std::move(v.filesystem),
                                                  v.total, v.free, v.available, v.read_only});
    }
    return out;
}

Result<server_info::Processes> ServerInfo::processes(const server_info::ProcessesRequest& request, Operation&) {
    server_info::Processes out;
    auto list = platform::processes(out.total);
    std::ranges::sort(
        list, [](const auto& a, const auto& b) { return a.memory != b.memory ? a.memory > b.memory : a.pid < b.pid; });
    const std::size_t n = std::min<std::size_t>(list.size(), std::min<std::uint32_t>(request.limit, 1000));
    for (std::size_t i = 0; i < n; ++i) {
        out.processes.push_back(
            server_info::Process{list[i].pid, std::move(list[i].name), list[i].memory, list[i].cpu_ms});
    }
    return out;
}

}  // namespace paglets::services::impl
