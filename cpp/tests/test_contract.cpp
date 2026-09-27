// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Service contracts (WP10): the host serves a contract by reflection, a
// guest calls it through the generated client and serves the same contract
// through the generated `serve`; descriptors agree.

#include "runtime_fixture.hpp"
#include "test.hpp"

#include <calc_contract.hpp>
#include <paglets/services/contract.hpp>
#include <paglets/wire/reflect.hpp>
#include <paglets/wire/schema.hpp>

using namespace paglets::test;
namespace ps = paglets::services;
namespace wire = paglets::wire;

namespace {

struct Calc final : ps::ContractPaglet<calc::Contract, Calc> {
    std::vector<std::string> default_ops() const override { return {"add", "divide", "describe"}; }

    ps::Result<calc::AddReply> add(const calc::AddRequest& q, ps::Operation&) { return calc::AddReply{q.a + q.b}; }

    ps::Result<calc::DivReply> divide(const calc::DivRequest& q, ps::Operation& op) {
        if (q.b == 0) return std::unexpected(abi::invalid_argument);
        const std::string caller = op.sender() ? op.sender()->id.substr(0, 4) : "none";
        return calc::DivReply{q.a / q.b, "host for " + caller};
    }
};

template <class T>
T from(const rt::Reply& r) {
    T v{};
    if (r.status != 0 || !wire::from_msgpack(r.payload, v)) throw std::runtime_error("bad reply");
    return v;
}

}  // namespace

PAGLETS_TEST("contracts: descriptors from reflection") {
    const auto desc = wire::namespace_descriptor<^^calc>();
    const auto* services = desc.find("services");
    REQUIRE(services != nullptr && services->as_array().size() == 1);
    const auto& svc = services->as_array()[0];
    CHECK(svc.find("name")->as_string() == "calc");
    const auto& ops = svc.find("operations")->as_array();
    REQUIRE(ops.size() == 2);
    CHECK(ops[0].find("name")->as_string() == "add");
    CHECK(ops[0].find("request")->as_string() == "AddRequest");
    CHECK(ops[1].find("reply")->as_string() == "DivReply");
    CHECK(desc.find("namespace")->as_string() == "calc");
    CHECK(desc.find("types")->as_array().size() == 4);  // the contract is not a wire type
}

PAGLETS_TEST("contracts: host service, generated guest client and guest service") {
    Fixture f;
    REQUIRE_OK(f.runtime->add_system_paglet(std::make_shared<Calc>()));
    const auto id = f.create("calc_user.wasm");

    // The guest serves the contract (generated serve()).
    CHECK(from<calc::AddReply>(f.call(id, "add", wire::to_msgpack(calc::AddRequest{2, 40}))).sum == 42);
    const auto guest_div = from<calc::DivReply>(f.call(id, "divide", wire::to_msgpack(calc::DivRequest{9, 3})));
    CHECK(guest_div.quotient == 3 && guest_div.served_by == "guest");
    CHECK(f.call(id, "divide", wire::to_msgpack(calc::DivRequest{1, 0})).status == abi::invalid_argument);

    // The guest calls the host's service (generated client, reflection dispatch).
    const auto host_div = from<calc::DivReply>(f.call(id, "use_service", wire::to_msgpack(calc::DivRequest{10, 5})));
    CHECK(host_div.quotient == 2);
    CHECK(host_div.served_by == "host for " + id.substr(0, 4));
    std::string failure;
    REQUIRE(wire::from_msgpack(f.call(id, "use_service", wire::to_msgpack(calc::DivRequest{1, 0})).payload, failure));
    CHECK(failure == "error:invalid_argument");

    // Both describe the same schema.
    std::string host_desc;
    std::string guest_desc;
    const auto sys = f.runtime->system_paglet("calc");
    REQUIRE(sys.has_value());
    REQUIRE(wire::from_msgpack(f.runtime->call(*sys, "describe").payload, host_desc));
    REQUIRE(wire::from_msgpack(f.call(id, "describe").payload, guest_desc));
    CHECK(host_desc == guest_desc);
    CHECK(host_desc == wire::namespace_descriptor<^^calc>().dump());
    // Unknown operations and undecodable requests.
    CHECK(f.runtime->call(*sys, "subtract").status == abi::unknown_message);
    CHECK(f.runtime->call(*sys, "add", Bytes{0xc1}).status == abi::malformed);
}
