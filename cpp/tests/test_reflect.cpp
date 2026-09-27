// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include "test.hpp"

#include <messages.hpp>
#include <paglets/wire/reflect.hpp>

#include <cmath>

using namespace counter_msgs;
namespace wire = paglets::wire;

namespace {

Echo sample() {
    Echo e;
    e.flag = true;
    e.tiny = -7;
    e.medium = -123456;
    e.big = 18446744073709551615ull;
    e.ratio = 0.25f;
    e.precise = 3.141592653589793;
    e.text = "Grüße \"quoted\"";
    e.blob = {0, 1, 2, 250, 255};
    e.items = {{5, "five", Mode::add}, {-2, "", Mode::multiply}};
    e.label = "label";
    e.nested = Status{42, 3, {"a", "b"}, 1.5, 17};
    e.mode = Mode::multiply;
    return e;
}

bool same(const Increment& a, const Increment& b) {
    return a.by == b.by && a.note == b.note && a.mode == b.mode;
}

bool same(const Echo& a, const Echo& b) {
    if (a.items.size() != b.items.size()) return false;
    for (std::size_t i = 0; i < a.items.size(); ++i) {
        if (!same(a.items[i], b.items[i])) return false;
    }
    const bool nested_same =
        a.nested.has_value() == b.nested.has_value() &&
        (!a.nested || (a.nested->value == b.nested->value && a.nested->recent_notes == b.nested->recent_notes &&
                       a.nested->average_step == b.nested->average_step));
    return a.flag == b.flag && a.tiny == b.tiny && a.medium == b.medium && a.big == b.big && a.ratio == b.ratio &&
           a.precise == b.precise && a.text == b.text && a.blob == b.blob && a.label == b.label && nested_same &&
           a.mode == b.mode;
}

}  // namespace

PAGLETS_TEST("reflect: msgpack round trip of all field types") {
    const Echo in = sample();
    Echo out;
    CHECK(wire::from_msgpack(wire::to_msgpack(in), out));
    CHECK(same(in, out));
}

PAGLETS_TEST("reflect: json round trip of all field types") {
    const Echo in = sample();
    const std::string json = wire::to_json(in);
    Echo out;
    CHECK(wire::from_json(json, out));
    CHECK(same(in, out));
    CHECK(json.find("\"mode\":\"multiply\"") != std::string::npos);
}

PAGLETS_TEST("reflect: unknown fields are skipped, missing fields keep defaults") {
    paglets::msgpack::Writer w;
    w.write_map_header(3);
    w.write_str("future_field");
    w.write_array_header(2);
    w.write_int(1);
    w.write_str("x");
    w.write_str("by");
    w.write_int(9);
    w.write_str("another");
    w.write_nil();
    Increment inc;
    CHECK(wire::from_msgpack(w.bytes(), inc));
    CHECK_EQ(inc.by, 9);
    CHECK(inc.mode == Mode::add);
}

PAGLETS_TEST("reflect: canonical schema names") {
    CHECK_EQ(wire::schema_name<Echo>(), std::string_view("Echo"));
    CHECK_EQ(wire::schema_name<std::vector<std::optional<std::int64_t>>>(), std::string_view("list<optional<i64>>"));
    CHECK_EQ(wire::schema_name<std::vector<std::uint8_t>>(), std::string_view("bytes"));
    CHECK_EQ(wire::enum_name(Mode::multiply), std::string_view("multiply"));
}
