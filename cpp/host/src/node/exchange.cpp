// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Content exchange between hosts (planning/cpp-content-exchange.md). An
// artifact capability names the host that holds its content and a token
// that host made: the SHA-256 of the host's signature over the content
// hash, so only the host can make it and needs no table to check it. A host
// that lacks the content pulls it from there in parts of 1 MB, a few parts
// ahead, writes it to a file as it arrives and checks the hash at the end.
// The holding host serves enrolled hosts only, only with a valid token,
// and only within the data residency of the content.

#include "node_impl.hpp"

#include <paglets/sha256.hpp>

namespace paglets::node {

std::string Node::Impl::content_token(std::string_view hash) const {
    constexpr std::string_view context = "paglets content token\n";
    std::vector<std::uint8_t> message(context.begin(), context.end());
    message.insert(message.end(), hash.begin(), hash.end());
    const mesh::Signature signature = key.sign(message);
    return to_hex(paglets::sha256(signature)).substr(0, 32);
}

void Node::Impl::install_exchange() {
    services::ContentExchange exchange;
    exchange.host = key.id();
    exchange.token = [this](std::string_view hash) { return content_token(hash); };
    exchange.fetch = [this](services::ContentFetch q, std::function<void(services::ContentFetched)> done) {
        std::lock_guard lock(mu);
        start_pull(std::move(q), std::move(done));
    };
    services->set_content_exchange(std::move(exchange));
}

std::optional<std::string> Node::Impl::residency_refusal(const std::vector<std::string>& marks,
                                                         const mesh::PublicKey& to) const {
    const mesh::LedgerState& st = ledger.state();
    for (const auto& mark : marks) {
        if (!mark.starts_with("root:")) continue;
        auto rule = st.residency.find(mark.substr(5));
        if (rule == st.residency.end()) continue;
        if (rule->second.host_only) {
            return "data residency: the content of root " + rule->first + " stays on its host";
        }
        if (!mesh::selects(st, rule->second.hosts, to)) {
            auto host = st.hosts.find(to);
            return "data residency: the content of root " + rule->first + " may not go to host " +
                   (host == st.hosts.end() ? mesh::key_id(to).substr(0, 16) : host->second.name);
        }
    }
    return std::nullopt;
}

// -- the host that lacks the content -------------------------------------------

void Node::Impl::start_pull(services::ContentFetch q, std::function<void(services::ContentFetched)> done) {
    auto fail = [&](std::int32_t status, std::string why) {
        done(services::ContentFetched{status, std::move(why), 0, {}});
    };
    auto host = mesh::parse_key_id(q.host);
    if (!host) return fail(abi::invalid_argument, "invalid host " + q.host);
    if (*host == key.public_key()) return fail(abi::not_found, "the content is not on this host");
    if (!ledger.state().is_host(*host)) return fail(abi::not_found, "host " + q.host.substr(0, 16) + " is not in the mesh");
    ContentPull p;
    p.peer = *host;
    p.hash = std::move(q.hash);
    p.token = std::move(q.token);
    p.into = std::move(q.into);
    p.max_bytes = q.max_bytes;
    p.out.open(p.into, std::ios::binary | std::ios::trunc);
    if (!p.out) return fail(abi::internal, "cannot write " + p.into.string());
    p.deadline = Clock::now() + pull_timeout;
    p.done = std::move(done);
    const std::uint64_t request = next_request++;
    pulls.emplace(request, std::move(p));
    ask_parts(request);
}

void Node::Impl::ask_parts(std::uint64_t request) {
    ContentPull& p = pulls.at(request);
    // Until the first part tells the size, only the first is asked for.
    const std::uint64_t end = p.total < 0 ? content_part_bytes : static_cast<std::uint64_t>(p.total);
    while (p.asked < end && p.asked - p.received < content_window * content_part_bytes) {
        send_frame(p.peer, mesh::Map{{"t", mesh::Value(frame_content_want)},
                                     {"r", mesh::Value(static_cast<std::int64_t>(request))},
                                     {"h", mesh::Value(p.hash)},
                                     {"k", mesh::Value(p.token)},
                                     {"o", mesh::Value(static_cast<std::int64_t>(p.asked))}});
        p.asked = std::min<std::uint64_t>(p.asked + content_part_bytes, end);
    }
}

void Node::Impl::finish_pull(std::uint64_t request, services::ContentFetched result) {
    auto node = pulls.extract(request);
    if (node.empty()) return;
    ContentPull& p = node.mapped();
    p.out.close();
    if (result.status != abi::ok) {
        std::error_code ec;
        fs::remove(p.into, ec);
    }
    p.done(std::move(result));
}

void Node::Impl::on_content_part(const mesh::PublicKey& from, const mesh::Fields& f) {
    auto r = f.integer("r");
    auto offset = f.integer("o");
    auto total = f.integer("n");
    auto data = f.bin("d");
    if (!r) return;
    const auto request = static_cast<std::uint64_t>(*r);
    auto it = pulls.find(request);
    if (it == pulls.end() || it->second.peer != from) return;
    ContentPull& p = it->second;
    if (!offset || !total || !data || *total < 0 || (p.total >= 0 && p.total != *total) ||
        *offset != static_cast<std::int64_t>(p.received)) {
        return finish_pull(request, {abi::malformed, "malformed content transfer", 0, {}});
    }
    if (static_cast<std::uint64_t>(*total) > p.max_bytes) {
        return finish_pull(request, {abi::too_large, "the content (" + std::to_string(*total) + " bytes) does not fit",
                                     0, {}});
    }
    const std::uint64_t expected =
        std::min<std::uint64_t>(content_part_bytes, static_cast<std::uint64_t>(*total) - p.received);
    if (data->size() != expected) return finish_pull(request, {abi::malformed, "malformed content transfer", 0, {}});
    if (p.total < 0) {
        p.total = *total;
        if (auto marks = f.strings("s")) p.marks = std::move(*marks);
    }
    if (!data->empty() &&
        !p.out.write(reinterpret_cast<const char*>(data->data()), static_cast<std::streamsize>(data->size()))) {
        return finish_pull(request, {abi::internal, "cannot write " + p.into.string(), 0, {}});
    }
    p.digest.update(*data);
    p.received += data->size();
    p.deadline = Clock::now() + pull_timeout;
    if (p.received < static_cast<std::uint64_t>(p.total)) return ask_parts(request);
    p.out.flush();
    if (!p.out) return finish_pull(request, {abi::internal, "cannot write " + p.into.string(), 0, {}});
    if (to_hex(p.digest.finish()) != p.hash) {
        return finish_pull(request, {abi::failed, "the content does not match its SHA-256", 0, {}});
    }
    finish_pull(request, {abi::ok, {}, p.received, p.marks});
}

void Node::Impl::on_content_missing(const mesh::PublicKey& from, const mesh::Fields& f) {
    auto r = f.integer("r");
    if (!r) return;
    auto it = pulls.find(static_cast<std::uint64_t>(*r));
    if (it == pulls.end() || it->second.peer != from) return;
    std::string why = f.str("e").value_or("the content is not there");
    const std::int32_t status = why.starts_with("data residency") || why.starts_with("invalid token") ? abi::denied
                                                                                                      : abi::not_found;
    finish_pull(it->first, {status, "host " + mesh::key_id(from).substr(0, 16) + ": " + why, 0, {}});
}

void Node::Impl::check_pull_deadlines() {
    const auto now = Clock::now();
    std::vector<std::uint64_t> late;
    for (const auto& [request, p] : pulls) {
        if (now >= p.deadline) late.push_back(request);
    }
    for (auto request : late) finish_pull(request, {abi::timeout, "no answer from the host in time", 0, {}});
}

// -- the host that holds the content ---------------------------------------------

void Node::Impl::on_content_want(const mesh::PublicKey& from, const mesh::Fields& f) {
    auto r = f.integer("r");
    auto hash = f.str("h");
    auto token = f.str("k");
    auto offset = f.integer("o");
    if (!r || !hash || !token || !offset || *offset < 0) return;
    auto missing = [&](const std::string& why) {
        send_frame(from, mesh::Map{{"t", mesh::Value(frame_content_missing)},
                                   {"r", mesh::Value(*r)},
                                   {"e", mesh::Value(why)}});
    };
    if (!ledger.state().is_host(from)) return missing("not an enrolled host");
    if (*token != content_token(*hash)) return missing("invalid token");
    auto content = services->served_content(*hash);
    if (!content) return missing("the content is not here");
    if (auto why = residency_refusal(content->marks, from)) return missing(*why);
    const auto at = static_cast<std::uint64_t>(*offset);
    if (at > content->size) return missing("offset beyond the content");
    const std::size_t n = static_cast<std::size_t>(std::min<std::uint64_t>(content_part_bytes, content->size - at));
    runtime::Bytes data(n);
    std::ifstream in(content->file, std::ios::binary);
    in.seekg(static_cast<std::streamoff>(at));
    if (!in || (n > 0 && !in.read(reinterpret_cast<char*>(data.data()), static_cast<std::streamsize>(n)))) {
        return missing("the content cannot be read");
    }
    mesh::Map part{{"t", mesh::Value(frame_content_part)},
                   {"r", mesh::Value(*r)},
                   {"o", mesh::Value(*offset)},
                   {"n", mesh::Value(static_cast<std::int64_t>(content->size))},
                   {"d", mesh::Value(std::move(data))}};
    if (at == 0) {
        mesh::Array marks;
        for (const auto& m : content->marks) marks.emplace_back(m);
        part.emplace_back("s", mesh::Value(std::move(marks)));
    }
    send_frame(from, std::move(part));
}

}  // namespace paglets::node
