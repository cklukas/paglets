// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Internal to the system paglets: their classes and shared helpers.

#pragma once

#include <paglets/services/artifacts.hpp>
#include <paglets/services/contract.hpp>
#include <paglets/services/directory.hpp>
#include <paglets/services/files.hpp>
#include <paglets/services/pubsub.hpp>
#include <paglets/services/server_info.hpp>
#include <paglets/services/storage.hpp>
#include <paglets/services/system_services.hpp>
#include <paglets/services/user_info.hpp>

#include "../platform/platform.hpp"

#include <deque>
#include <filesystem>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

namespace paglets::services::impl {

namespace fs = std::filesystem;

// -- helpers ------------------------------------------------------------------

// A relative path of a request: "" or `/`-separated segments, none empty,
// `.`, `..`, or containing `\` or `:` (the rules of dir capabilities).
bool valid_relative(std::string_view path, bool allow_empty);
// Joins relative paths ("" is neutral).
std::string join_relative(std::string_view a, std::string_view b);
// 1 to `max` characters from [a-z0-9._-] (with `upper`: also A-Z).
bool valid_name(std::string_view name, std::size_t max, bool upper = false);
std::string random_hex(std::size_t bytes);
std::int64_t now_ms();
std::expected<void, std::string> write_atomically(const fs::path& path, std::span<const std::uint8_t> data);
std::expected<std::vector<std::uint8_t>, std::string> read_all(const fs::path& path);
// The caller of a message; nullopt for the host itself and timers.
std::optional<abi::SenderRecord> caller_of(const runtime::ServiceCall& call);

// -- server-info ----------------------------------------------------------------

class ServerInfo final : public ContractPaglet<server_info::Contract, ServerInfo> {
public:
    bool ambient() const override { return true; }
    void start(runtime::SystemContext& ctx) override;
    Result<server_info::Summary> summary(const server_info::SummaryRequest&, Operation&);
    Result<server_info::Load> load(const server_info::LoadRequest&, Operation&);
    Result<server_info::Volumes> volumes(const server_info::VolumesRequest&, Operation&);
    Result<server_info::Processes> processes(const server_info::ProcessesRequest&, Operation&);

private:
    std::mutex mu_;
    platform::CpuSample last_;
};

// -- files ----------------------------------------------------------------------

class Files final : public ContractPaglet<files::Contract, Files> {
public:
    explicit Files(std::map<std::string, fs::path> roots);

    const std::map<std::string, fs::path>& roots() const { return roots_; }

    Result<files::ListReply> list(const files::ListRequest&, Operation&);
    Result<files::Entry> stat(const files::StatRequest&, Operation&);
    Result<files::FindReply> find(const files::FindRequest&, Operation&);
    Result<files::ReadReply> read(const files::ReadRequest&, Operation&);
    Result<files::Entry> write(const files::WriteRequest&, Operation&);
    Result<files::Entry> mkdir(const files::MkdirRequest&, Operation&);
    Result<files::Entry> move(const files::MoveRequest&, Operation&);
    Result<files::DeleteReply> remove(const files::DeleteRequest&, Operation&);

    static constexpr std::uint64_t max_transfer = 4u * 1024 * 1024;

private:
    struct Resolved {
        fs::path base;    // the directory of the capability (canonical)
        fs::path path;    // the requested path below it
        std::string rel;  // the requested path, relative to the capability
    };
    // Checks the lent capability for `right` and resolves `rel` below it.
    Result<Resolved> resolve(Operation& op, std::string_view right, std::string_view rel) const;
    Result<Resolved> resolve(Operation& op, std::initializer_list<std::string_view> rights, std::string_view rel) const;

    std::map<std::string, fs::path> roots_;
};

// -- storage --------------------------------------------------------------------

class Storage final : public ContractPaglet<storage::Contract, Storage> {
public:
    explicit Storage(std::uint64_t quota) : quota_(quota) {}
    std::vector<std::string> default_ops() const override { return operations(); }

    Result<storage::GetReply> get(const storage::GetRequest&, Operation&);
    Result<storage::Usage> put(const storage::PutRequest&, Operation&);
    Result<storage::DeleteReply> remove(const storage::DeleteRequest&, Operation&);
    Result<storage::ListReply> list(const storage::ListRequest&, Operation&);

private:
    Result<fs::path> space(Operation& op) const;
    storage::Usage usage(const fs::path& space) const;

    std::uint64_t quota_;
};

// -- directory ------------------------------------------------------------------

class Directory final : public ContractPaglet<directory::Contract, Directory> {
public:
    Directory(fs::path state_file, ServicePolicy policy);
    std::vector<std::string> default_ops() const override { return operations(); }
    void start(runtime::SystemContext& ctx) override;
    void paglet_ended(runtime::SystemContext& ctx, const runtime::PagletId& id) override;
    void set_policy(ServicePolicy policy);

    Result<directory::LookupReply> lookup(const directory::LookupRequest&, Operation&);
    Result<directory::PublishReply> publish(const directory::PublishRequest&, Operation&);
    Result<directory::UnpublishReply> unpublish(const directory::UnpublishRequest&, Operation&);
    Result<directory::ListReply> list(const directory::ListRequest&, Operation&);

private:
    struct Published {
        std::string paglet;
        std::string owner;
        std::vector<std::string> ops;
        bool is_public = false;
    };
    bool visible(const Published& p, const abi::SenderRecord& caller) const;
    void save();  // mu_ held

    mutable std::mutex mu_;
    fs::path state_file_;
    ServicePolicy policy_;
    std::map<std::string, Published> names_;
    runtime::Runtime* runtime_ = nullptr;
};

// -- artifacts ------------------------------------------------------------------

class Artifacts final : public ContractPaglet<artifacts::Contract, Artifacts> {
public:
    Artifacts(fs::path root, bool temporary, std::uint64_t limit);
    ~Artifacts() override;

    Result<artifacts::Info> put(const artifacts::PutRequest&, Operation&);
    Result<artifacts::GetReply> get(const artifacts::GetRequest&, Operation&);
    Result<artifacts::Info> stat(const artifacts::StatRequest&, Operation&);
    // Stores content (deduplicated by hash; marks are added to an existing
    // artifact's); returns its hash.
    Result<std::string> store(std::span<const std::uint8_t> data, std::string_view media_type,
                              const std::vector<std::string>& marks);

private:
    Result<artifacts::Info> info_of(std::string_view hash) const;
    Result<std::string> lent_hash(Operation& op) const;

    std::mutex mu_;
    fs::path root_;
    bool temporary_;
    std::uint64_t limit_;
};

// -- pubsub ---------------------------------------------------------------------

class PubSub final : public ContractPaglet<pubsub::Contract, PubSub> {
public:
    explicit PubSub(fs::path state_file);
    void paglet_ended(runtime::SystemContext& ctx, const runtime::PagletId& id) override;

    Result<pubsub::Topic> create(const pubsub::CreateRequest&, Operation&);
    Result<pubsub::PublishReply> publish(const pubsub::PublishRequest&, Operation&);
    Result<pubsub::SubscribeReply> subscribe(const pubsub::SubscribeRequest&, Operation&);
    Result<pubsub::UnsubscribeReply> unsubscribe(const pubsub::UnsubscribeRequest&, Operation&);

private:
    struct Topic {
        std::string name;
        std::map<std::string, std::string> subscribers;  // paglet -> message name
    };
    Result<std::string> lent_topic(Operation& op, std::string_view right) const;
    void save();  // mu_ held
    void load();

    std::mutex mu_;
    fs::path state_file_;
    std::map<std::string, Topic> topics_;
};

// -- user-info ------------------------------------------------------------------

class UserInfo final : public ContractPaglet<user_info::Contract, UserInfo> {
public:
    UserInfo(std::size_t per_owner, std::function<void(const Notification&)> sink);
    std::vector<std::string> default_ops() const override { return operations(); }

    Result<user_info::NotifyReply> notify(const user_info::NotifyRequest&, Operation&);
    std::vector<Notification> notifications(std::string_view owner) const;

private:
    mutable std::mutex mu_;
    std::size_t per_owner_;
    std::function<void(const Notification&)> sink_;
    std::map<std::string, std::deque<Notification>, std::less<>> by_owner_;
    std::uint64_t next_id_ = 1;
};

}  // namespace paglets::services::impl
