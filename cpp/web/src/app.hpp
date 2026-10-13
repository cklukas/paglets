// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// paglets-web (WP35, planning/cpp-web-ui.md): the mesh as a JSON model kept
// fresh by a poller, the JSON API with the actions, and the UI's assets.

#pragma once

#include <paglets/client/client.hpp>
#include <paglets/web/http.hpp>
#include <paglets/wire/json.hpp>

#include <atomic>
#include <condition_variable>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

namespace paglets::webapp {

struct Options {
    std::string connect;      // the entry host
    std::string key_file;
    std::string ledger_dir;
    std::optional<std::string> host_key;
    std::optional<std::string> passphrase_file;
    std::optional<std::filesystem::path> module_dir;
    std::chrono::milliseconds refresh{2000};
    std::string program = "paglets-host";  // in shell commands
};

class App {
public:
    App(Options options, client::Identity identity);
    ~App();

    // Polls the mesh until stop(): hosts and landscape from the entry
    // host, status from every host it can reach.
    void run_poller();
    void stop();
    // One refresh now (tests, and the first page).
    void refresh();

    // The HTTP routes below /api/ (the guard has checked the request).
    web::Response api(const web::Request& request);

    // The current model as JSON text.
    std::string model() const;

private:
    struct HostLink;

    web::Response mesh_route() const;
    web::Response host_route(const web::Request& request);
    web::Response paglet_route(const web::Request& request);
    web::Response modules_route();
    web::Response zombies_route();
    web::Response action(const std::string& name, const wire::Json& body);

    // A session with a host for an action (a new one each time, so actions
    // run in parallel); the URL and key come from the entry host's registry.
    std::expected<client::Session, std::string> open_session(const std::string& host_key);
    std::optional<std::pair<std::string, mesh::PublicKey>> host_address(const std::string& host_key) const;
    client::ShellContext shell(const std::string& host_key) const;

    Options options_;
    client::Identity identity_;
    mutable std::mutex mu;  // model_, hosts_, version_
    wire::Json model_;
    std::uint64_t version_ = 0;
    std::string content_;  // the model without version and time, as text
    std::map<std::string, std::string> urls_;  // host key ID -> URL
    std::string entry_key_;                    // key ID of the entry host

    std::mutex links_mu;
    std::map<std::string, std::shared_ptr<HostLink>> links_;  // poll sessions by host key ID

    std::atomic<bool> stopping_{false};
    std::mutex wait_mu;
    std::condition_variable wait_cv;
};

// The UI: one page, its stylesheet and script.
extern const std::string_view index_html;
extern const std::string_view app_css;
extern const std::string_view app_js;

// Routes everything: the token exchange, the assets, the API.
web::Response route(App& app, const web::Guard& guard, const web::Request& request);

}  // namespace paglets::webapp
