# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from paglets.examples.compute import (
    PiBatchResult,
    PiComputeRequest,
    chudnovsky_binary_split,
    pi_decimal,
    pi_decimal_digits,
    pi_decimal_digits_from_results,
)
from paglets.examples.compute.chudnovsky import _decode_bigint, _encode_bigint, _int_to_decimal_string


def test_pi_decimal_digits_are_deterministic():
    assert pi_decimal(0, 16) == "3.1415926535897932"
    assert pi_decimal_digits(0, 16) == "1415926535897932"


def test_large_decimal_formatting_avoids_python_int_string_limit():
    text = pi_decimal(0, 4310)

    assert text.startswith("3.1415926535897932")
    assert len(text) == 4312


def test_bigint_wire_helpers_avoid_python_decimal_string_limit():
    decimal_text = "1" + ("0" * 5000)
    value = _decode_bigint(decimal_text)
    encoded = _encode_bigint(-value)

    assert encoded.startswith("-0x")
    assert _decode_bigint(encoded) == -value
    assert _int_to_decimal_string(value) == decimal_text


def test_pi_digits_can_be_formatted_from_batch_results_locally():
    request = PiComputeRequest(start=0, digits=8, batch_size=1, timeout=5.0, max_cpu_percent=100.0)
    p, q, t = chudnovsky_binary_split(0, 1)
    result = PiBatchResult(
        batch_id="terms:0:1",
        term_start=0,
        term_count=1,
        host_name="alpha",
        host_url="http://127.0.0.1:1",
        status="ok",
        p=str(p),
        q=str(q),
        t=str(t),
    )

    assert pi_decimal_digits_from_results(request, [result], after_digits=0, digits=4) == "1415"
