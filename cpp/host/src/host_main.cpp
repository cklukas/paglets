// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// paglets-host: the paglets/cpp host binary (milestone M1: single host).
//
//   paglets-host --version | --info
//   paglets-host run <module.wasm> [--args JSON] [--call NAME [JSON]]...
//                    [--state-dir DIR] [--threads N] [--keep] [--in-process]
//   paglets-host list --state-dir DIR
//   paglets-host call --state-dir DIR <paglet-id|all> NAME [JSON] [--expect TEXT]
//
// Message bodies and arguments are given as JSON and passed to the paglet as
// MessagePack; replies are printed as JSON. With a state directory, paglets
// survive the host process: `run --keep` leaves them there, `list` and
// `call` resume them from their last memory image. Paglets run in worker
// processes (paglets-worker next to this binary) unless --in-process is given
// or no worker executable is found.

#include <paglets/abi.hpp>
#include <paglets/runtime/runtime.hpp>
#include <paglets/wasm/engine.hpp>
#include <paglets/wire/json_msgpack.hpp>

#include <filesystem>
#include <iostream>
#include <system_error>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#if defined(__APPLE__)
#include <mach-o/dyld.h>
#endif

namespace rt = paglets::runtime;
namespace abi = paglets::abi;
using paglets::wire::Json;

namespace {

std::string_view compiler() {
#if defined(__clang__)
    return "clang " __clang_version__;
#elif defined(__GNUC__)
    return "gcc " __VERSION__;
#else
    return "unknown";
#endif
}

std::string_view platform() {
#if defined(__APPLE__)
    return "macos";
#elif defined(_WIN32)
    return "windows";
#elif defined(__linux__)
    return "linux";
#else
    return "unknown";
#endif
}

std::string_view architecture() {
#if defined(__aarch64__) || defined(_M_ARM64)
    return "arm64";
#elif defined(__x86_64__) || defined(_M_X64)
    return "x86-64";
#else
    return "unknown";
#endif
}

int usage() {
    std::cerr << "usage: paglets-host --version | --info\n"
                 "       paglets-host run <module.wasm> [--args JSON] [--call NAME [JSON]]...\n"
                 "                        [--state-dir DIR] [--threads N] [--keep] [--in-process]\n"
                 "       paglets-host list --state-dir DIR\n"
                 "       paglets-host call --state-dir DIR <paglet-id|all> NAME [JSON] [--expect TEXT]\n";
    return 2;
}

int fail(const std::string& message) {
    std::cerr << "paglets-host: " << message << "\n";
    return 1;
}

int info() {
    paglets::wasm::ensure_runtime();
    std::cout << "paglets-host " << PAGLETS_VERSION << " (milestone M1)\n"
              << "  platform:   " << platform() << " " << architecture() << "\n"
              << "  compiler:   " << compiler() << "\n"
              << "  reflection: "
#if PAGLETS_HAVE_REFLECTION
              << "yes"
#else
              << "no"
#endif
              << "\n"
              << "  runtime:    " << PAGLETS_WAMR_TAG << " (" << PAGLETS_WAMR_MODE << ")\n"
              << "  paglet ABI: v" << abi::version << "\n";
    return 0;
}

struct Call {
    std::string name;
    std::optional<std::string> body;  // JSON
};

struct Options {
    std::vector<std::string> positional;
    std::vector<Call> calls;
    std::optional<std::string> args;
    std::optional<std::string> state_dir;
    std::optional<std::string> expect;
    unsigned threads = 2;
    bool keep = false;
    bool in_process = false;
};

std::optional<Options> parse(int argc, char** argv) {
    Options o;
    auto value = [&](int& i) -> std::optional<std::string> {
        if (i + 1 >= argc) return std::nullopt;
        return std::string(argv[++i]);
    };
    for (int i = 2; i < argc; ++i) {
        const std::string_view a = argv[i];
        if (a == "--call") {
            auto name = value(i);
            if (!name) return std::nullopt;
            Call c{*name, std::nullopt};
            if (i + 1 < argc && !std::string_view(argv[i + 1]).starts_with("--")) c.body = argv[++i];
            o.calls.push_back(std::move(c));
        } else if (a == "--args") {
            if (!(o.args = value(i))) return std::nullopt;
        } else if (a == "--state-dir") {
            if (!(o.state_dir = value(i))) return std::nullopt;
        } else if (a == "--expect") {
            if (!(o.expect = value(i))) return std::nullopt;
        } else if (a == "--threads") {
            auto v = value(i);
            if (!v) return std::nullopt;
            o.threads = static_cast<unsigned>(std::stoul(*v));
        } else if (a == "--keep") {
            o.keep = true;
        } else if (a == "--in-process") {
            o.in_process = true;
        } else if (a.starts_with("--")) {
            return std::nullopt;
        } else {
            o.positional.emplace_back(a);
        }
    }
    return o;
}

std::optional<std::vector<std::uint8_t>> body_of(const std::optional<std::string>& json) {
    if (!json) return std::vector<std::uint8_t>{};
    auto parsed = Json::parse(*json);
    if (!parsed) return std::nullopt;
    return paglets::wire::json_to_msgpack(*parsed);
}

std::string show(const rt::Reply& reply) {
    if (reply.status != 0) return "error: " + std::string(abi::error_name(reply.status));
    if (reply.payload.empty()) return "ok";
    auto json = paglets::wire::msgpack_to_json(reply.payload);
    return json ? json->dump() : "(" + std::to_string(reply.payload.size()) + " bytes, not MessagePack)";
}

std::filesystem::path self_path;

// The running executable, to find paglets-worker next to it.
std::filesystem::path executable_path(const char* argv0) {
    std::error_code ec;
#if defined(__linux__)
    if (auto exe = std::filesystem::read_symlink("/proc/self/exe", ec); !ec) return exe;
#elif defined(__APPLE__)
    std::uint32_t size = 0;
    _NSGetExecutablePath(nullptr, &size);
    std::string buf(size, '\0');
    if (_NSGetExecutablePath(buf.data(), &size) == 0) {
        auto exe = std::filesystem::weakly_canonical(std::filesystem::path(buf.c_str()), ec);
        if (!ec) return exe;
    }
#endif
    auto p = std::filesystem::weakly_canonical(std::filesystem::path(argv0), ec);
    return ec ? std::filesystem::path() : p;
}

rt::Config config_of(const Options& o) {
    rt::Config c;
    c.threads = o.threads;
    if (o.state_dir) c.state_dir = *o.state_dir;
    if (!o.in_process && !self_path.empty()) {
        auto worker = self_path.parent_path() / "paglets-worker";
#ifdef _WIN32
        worker += ".exe";
#endif
        std::error_code ec;
        if (std::filesystem::exists(worker, ec)) c.worker_executable = worker;
    }
    c.log = [](const rt::LogRecord& r) {
        std::cerr << "[" << (r.paglet.empty() ? std::string("host") : r.paglet.substr(0, 8)) << "] " << r.text << "\n";
    };
    return c;
}

int cmd_run(const Options& o) {
    if (o.positional.size() != 1) return usage();
    rt::Runtime runtime(config_of(o));
    auto module = runtime.add_module_file(o.positional[0]);
    if (!module) return fail(module.error());
    auto args = body_of(o.args);
    if (!args) return fail("--args is not valid JSON");
    auto id = runtime.create(*module, rt::CreateOptions{std::move(*args)});
    if (!id) return fail(id.error());
    std::cout << "paglet " << *id << " (module " << module->substr(0, 16) << ")\n";
    int status = 0;
    for (const auto& c : o.calls) {
        auto body = body_of(c.body);
        if (!body) return fail("body of " + c.name + " is not valid JSON");
        const auto reply = runtime.call(*id, c.name, std::move(*body));
        std::cout << c.name << " -> " << show(reply) << "\n";
        if (reply.status != 0) status = 1;
    }
    runtime.wait_idle();
    if (!o.keep && runtime.info(*id)) {
        (void)runtime.dispose(*id);
        runtime.wait_idle();
    }
    return status;
}

int cmd_list(const Options& o) {
    if (!o.state_dir) return usage();
    rt::Runtime runtime(config_of(o));
    for (const auto& p : runtime.list()) {
        std::cout << p.id << "  " << rt::to_string(p.trust) << "  " << rt::to_string(p.state) << "  module "
                  << p.module.substr(0, 16) << "  owner " << p.owner << "\n";
    }
    return 0;
}

int cmd_call(const Options& o) {
    if (!o.state_dir || o.positional.size() < 2 || o.positional.size() > 3) return usage();
    rt::Runtime runtime(config_of(o));
    std::vector<rt::PagletId> targets;
    if (o.positional[0] == "all") {
        for (const auto& p : runtime.list()) targets.push_back(p.id);
    } else {
        targets.push_back(o.positional[0]);
    }
    if (targets.empty()) return fail("no paglets in " + *o.state_dir);
    std::optional<std::string> json;
    if (o.positional.size() == 3) json = o.positional[2];
    int status = 0;
    for (const auto& id : targets) {
        auto body = body_of(json);
        if (!body) return fail("the message body is not valid JSON");
        const auto reply = runtime.call(id, o.positional[1], std::move(*body));
        const std::string shown = show(reply);
        std::cout << id << " " << o.positional[1] << " -> " << shown << "\n";
        if (reply.status != 0) status = 1;
        if (o.expect && shown.find(*o.expect) == std::string::npos) {
            std::cerr << "paglets-host: expected " << *o.expect << "\n";
            status = 1;
        }
    }
    runtime.wait_idle();
    return status;
}

}  // namespace

int main(int argc, char** argv) {
    self_path = executable_path(argv[0]);
    const std::string_view cmd = argc > 1 ? argv[1] : "--info";
    if (cmd == "--version") {
        std::cout << "paglets-host " << PAGLETS_VERSION << "\n";
        return 0;
    }
    if (cmd == "--info") return info();
    auto options = parse(argc, argv);
    if (!options) return usage();
    try {
        if (cmd == "run") return cmd_run(*options);
        if (cmd == "list") return cmd_list(*options);
        if (cmd == "call") return cmd_call(*options);
    } catch (const std::exception& e) {
        return fail(e.what());
    }
    return usage();
}
