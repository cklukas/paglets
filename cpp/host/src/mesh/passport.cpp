// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/mesh/passport.hpp>

#include <algorithm>

namespace paglets::mesh {

namespace {

constexpr std::string_view root_domain = "paglets passport v1";
constexpr std::string_view link_domain = "paglets passport link v1";
constexpr std::size_t max_links = 64;

Bytes signing_message(std::string_view domain, const Bytes& body) {
    Bytes m(domain.begin(), domain.end());
    m.push_back(0);
    m.insert(m.end(), body.begin(), body.end());
    return m;
}

template <std::size_t N>
bool fixed(const Fields& f, std::string_view key, std::array<std::uint8_t, N>& out) {
    auto v = f.template fixed<N>(key);
    if (!v) return false;
    out = *v;
    return true;
}

std::expected<PassportRoot, std::string> parse_root(const Bytes& body) {
    auto v = decode(body);
    if (!v || v->as_map() == nullptr) return std::unexpected(std::string("malformed passport"));
    const Fields f{*v->as_map()};
    PassportRoot r;
    auto type = f.str("type");
    auto paglet = f.str("paglet");
    auto issued = f.integer("issued");
    auto expires = f.integer("expires");
    const Value* manifest = f.get("manifest");
    if (!type || *type != "passport" || !paglet || !issued || !expires || manifest == nullptr ||
        !fixed(f, "mesh", r.mesh) || !fixed(f, "owner", r.owner) || !fixed(f, "module", r.module)) {
        return std::unexpected(std::string("malformed passport"));
    }
    r.paglet = *paglet;
    r.issued = *issued;
    r.expires = *expires;
    r.manifest = *manifest;
    return r;
}

std::expected<std::pair<PassportLink, Digest>, std::string> parse_link(const Bytes& body) {
    auto v = decode(body);
    if (!v || v->as_map() == nullptr) return std::unexpected(std::string("malformed passport link"));
    const Fields f{*v->as_map()};
    PassportLink l;
    Digest prev{};
    auto type = f.str("type");
    auto kind = f.str("kind");
    auto parent = f.str("parent");
    auto paglet = f.str("paglet");
    auto issued = f.integer("issued");
    if (!type || *type != "passport-link" || !kind || (*kind != "child" && *kind != "clone") || !parent || !paglet ||
        !issued || !fixed(f, "module", l.module) || !fixed(f, "host", l.host) || !fixed(f, "prev", prev)) {
        return std::unexpected(std::string("malformed passport link"));
    }
    l.kind = *kind;
    l.parent = *parent;
    l.paglet = *paglet;
    l.issued = *issued;
    return std::pair{l, prev};
}

}  // namespace

std::expected<Passport, std::string> Passport::issue(const SigningKey& owner, const RecordId& mesh,
                                                     const Digest& module, std::string paglet, Value manifest,
                                                     std::int64_t issued, std::int64_t expires) {
    if (paglet.empty()) return std::unexpected(std::string("a passport needs a paglet ID"));
    if (expires <= issued) return std::unexpected(std::string("a passport must expire after it is issued"));
    Passport p;
    p.root_ = PassportRoot{mesh, owner.public_key(), module, std::move(paglet), std::move(manifest), issued, expires};
    p.root_element_.body = mesh::encode(Value(Map{{"type", Value("passport")},
                                                  {"mesh", Value::bin(mesh)},
                                                  {"owner", Value::bin(owner.public_key())},
                                                  {"module", Value::bin(module)},
                                                  {"paglet", Value(p.root_.paglet)},
                                                  {"manifest", p.root_.manifest},
                                                  {"issued", Value(issued)},
                                                  {"expires", Value(expires)}}));
    p.root_element_.signature = owner.sign(signing_message(root_domain, p.root_element_.body));
    return p;
}

std::expected<Passport, std::string> Passport::extend(const SigningKey& host, std::string kind, std::string paglet,
                                                      const Digest& module, std::int64_t issued) const {
    if (kind != "child" && kind != "clone") return std::unexpected(std::string("passport links are child or clone"));
    if (paglet.empty()) return std::unexpected(std::string("a passport needs a paglet ID"));
    if (links_.size() >= max_links) return std::unexpected(std::string("passport chain too long"));
    Passport p = *this;
    const Bytes& previous = link_elements_.empty() ? root_element_.body : link_elements_.back().body;
    PassportLink l{std::move(kind), this->paglet(), std::move(paglet), module, host.public_key(), issued};
    Element e;
    e.body = mesh::encode(Value(Map{{"type", Value("passport-link")},
                                    {"kind", Value(l.kind)},
                                    {"parent", Value(l.parent)},
                                    {"paglet", Value(l.paglet)},
                                    {"module", Value::bin(l.module)},
                                    {"host", Value::bin(l.host)},
                                    {"prev", Value::bin(sha256(previous))},
                                    {"issued", Value(issued)}}));
    e.signature = host.sign(signing_message(link_domain, e.body));
    p.links_.push_back(std::move(l));
    p.link_elements_.push_back(std::move(e));
    return p;
}

Bytes Passport::encode() const {
    auto element = [](const Element& e) {
        return Value(Map{{"body", Value(e.body)}, {"sig", Value::bin(e.signature)}});
    };
    Array links;
    for (const auto& e : link_elements_) links.push_back(element(e));
    return mesh::encode(Value(Map{{"root", element(root_element_)}, {"links", Value(std::move(links))}}));
}

std::expected<Passport, std::string> Passport::decode(std::span<const std::uint8_t> bytes) {
    auto v = mesh::decode(bytes);
    if (!v || v->as_map() == nullptr) return std::unexpected(std::string("malformed passport"));
    const Fields f{*v->as_map()};
    const Map* root = f.submap("root");
    const Array* links = f.array("links");
    if (root == nullptr || links == nullptr || links->size() > max_links) {
        return std::unexpected(std::string("malformed passport"));
    }
    auto element = [](const Value& value) -> std::optional<Element> {
        const Map* m = value.as_map();
        if (m == nullptr) return std::nullopt;
        const Fields ef{*m};
        auto body = ef.bin("body");
        auto sig = ef.fixed<64>("sig");
        if (!body || !sig) return std::nullopt;
        return Element{std::move(*body), *sig};
    };
    Passport p;
    auto root_element = element(Value(*root));
    if (!root_element) return std::unexpected(std::string("malformed passport"));
    p.root_element_ = std::move(*root_element);
    auto parsed_root = parse_root(p.root_element_.body);
    if (!parsed_root) return std::unexpected(parsed_root.error());
    p.root_ = std::move(*parsed_root);
    if (!verify(p.root_.owner, signing_message(root_domain, p.root_element_.body), p.root_element_.signature)) {
        return std::unexpected(std::string("passport not signed by its owner"));
    }
    const Bytes* previous = &p.root_element_.body;
    std::string parent = p.root_.paglet;
    for (const auto& item : *links) {
        auto e = element(item);
        if (!e) return std::unexpected(std::string("malformed passport link"));
        auto parsed = parse_link(e->body);
        if (!parsed) return std::unexpected(parsed.error());
        auto& [link, prev] = *parsed;
        if (prev != sha256(*previous) || link.parent != parent) {
            return std::unexpected(std::string("passport link does not continue the chain"));
        }
        if (!verify(link.host, signing_message(link_domain, e->body), e->signature)) {
            return std::unexpected(std::string("passport link not signed by its host"));
        }
        parent = link.paglet;
        p.links_.push_back(std::move(link));
        p.link_elements_.push_back(std::move(*e));
        previous = &p.link_elements_.back().body;
    }
    return p;
}

const std::string& Passport::paglet() const {
    return links_.empty() ? root_.paglet : links_.back().paglet;
}

const Digest& Passport::module() const {
    return links_.empty() ? root_.module : links_.back().module;
}

std::expected<void, std::string> verify_passport(const Passport& passport, const LedgerState& state,
                                                 std::string_view paglet, const Digest& module, std::int64_t now) {
    const PassportRoot& root = passport.root();
    if (root.mesh != state.mesh) return std::unexpected(std::string("passport of another mesh"));
    if (state.revoked_keys.contains(root.owner) || !state.is_owner(root.owner)) {
        return std::unexpected("passport owner " + key_id(root.owner) + " is not an enrolled owner");
    }
    if (now >= root.expires) return std::unexpected(std::string("passport expired"));
    for (const auto& link : passport.links()) {
        if (state.revoked_keys.contains(link.host) || !state.is_host(link.host)) {
            return std::unexpected("passport link by " + key_id(link.host) + ", which is not an enrolled host");
        }
    }
    if (passport.paglet() != paglet) return std::unexpected(std::string("passport of another paglet"));
    if (passport.module() != module) return std::unexpected(std::string("passport of another module"));
    return {};
}

}  // namespace paglets::mesh
