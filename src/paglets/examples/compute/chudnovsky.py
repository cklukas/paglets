# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import math
from typing import Any

from .models import (
    CHUDNOVSKY_A,
    CHUDNOVSKY_B,
    CHUDNOVSKY_C3_OVER_24,
    CHUDNOVSKY_DIGITS_PER_TERM,
    CHUDNOVSKY_GUARD_DIGITS,
    DECIMAL_CHUNK_BASE,
    DECIMAL_CHUNK_DIGITS,
    PiBatchResult,
    PiComputeRequest,
)


def pi_decimal_digits(start: int, digits: int) -> str:
    if start < 0:
        raise ValueError("start must be non-negative")
    if digits < 0:
        raise ValueError("digits must be non-negative")
    request = PiComputeRequest(start=start, digits=digits)
    p, q, t = chudnovsky_binary_split(0, _terms_for_request(request))
    return _format_pi_decimal(p, q, t, start=start, digits=digits, precision_digits=_precision_digits(request))[1]


def pi_decimal(start: int, digits: int) -> str:
    request = PiComputeRequest(start=max(0, start), digits=max(0, digits))
    p, q, t = chudnovsky_binary_split(0, _terms_for_request(request))
    return _format_pi_decimal(
        p, q, t, start=request.start, digits=request.digits, precision_digits=_precision_digits(request)
    )[0]


def pi_decimal_digits_from_results(
    request: PiComputeRequest,
    results: list[PiBatchResult],
    *,
    after_digits: int = 0,
    digits: int | None = None,
) -> str:
    after_digits = max(0, int(after_digits))
    contiguous = _contiguous_result_pieces(results)
    completed_terms = sum(piece.term_count for piece in contiguous)
    available = max(0, _available_decimal_digits(request, completed_terms) - after_digits)
    digit_count = available if digits is None else min(available, max(0, int(digits)))
    if digit_count <= 0:
        return ""
    pieces = _pieces_needed_for_digits(request, contiguous, after_digits + digit_count)
    p, q, t = _combine_result_parts(pieces)
    absolute_start = request.start + after_digits
    return _format_pi_decimal(
        p,
        q,
        t,
        start=absolute_start,
        digits=digit_count,
        precision_digits=max(
            CHUDNOVSKY_GUARD_DIGITS + 1,
            absolute_start + digit_count + CHUDNOVSKY_GUARD_DIGITS,
        ),
    )[1]


def chudnovsky_binary_split(a: int, b: int) -> tuple[int, int, int]:
    if b <= a:
        raise ValueError("term range cannot be empty")
    if b - a == 1:
        if a == 0:
            p = 1
            q = 1
        else:
            p = (6 * a - 5) * (2 * a - 1) * (6 * a - 1)
            q = a * a * a * CHUDNOVSKY_C3_OVER_24
        t = p * (CHUDNOVSKY_A + CHUDNOVSKY_B * a)
        if a % 2:
            t = -t
        return p, q, t
    middle = (a + b) // 2
    left = chudnovsky_binary_split(a, middle)
    right = chudnovsky_binary_split(middle, b)
    return _combine_parts(left, right)


def _combine_parts(left: tuple[int, int, int], right: tuple[int, int, int]) -> tuple[int, int, int]:
    p1, q1, t1 = left
    p2, q2, t2 = right
    return p1 * p2, q1 * q2, t1 * q2 + p1 * t2


def _combine_result_parts(results: list[PiBatchResult]) -> tuple[int, int, int]:
    combined: tuple[int, int, int] | None = None
    for result in sorted(results, key=lambda item: item.term_start):
        part = (_decode_bigint(result.p), _decode_bigint(result.q), _decode_bigint(result.t))
        combined = part if combined is None else _combine_parts(combined, part)
    if combined is None:
        raise ValueError("no Pi term results to combine")
    return combined


def _encode_bigint(value: int) -> str:
    return hex(value)


def _decode_bigint(value: str) -> int:
    text = value.strip()
    if text.lower().startswith(("0x", "+0x", "-0x")):
        return int(text, 16)
    return _parse_decimal_bigint(text)


def _parse_decimal_bigint(value: str) -> int:
    if not value:
        raise ValueError("empty integer")
    sign = 1
    digits = value
    if value[0] in "+-":
        sign = -1 if value[0] == "-" else 1
        digits = value[1:]
    if not digits or not digits.isdecimal():
        raise ValueError(f"invalid integer: {value!r}")
    number = 0
    for index in range(0, len(digits), DECIMAL_CHUNK_DIGITS):
        chunk = digits[index : index + DECIMAL_CHUNK_DIGITS]
        number = number * (10 ** len(chunk)) + int(chunk)
    return sign * number


def _int_to_decimal_string(value: int) -> str:
    if value == 0:
        return "0"
    sign = "-" if value < 0 else ""
    value = abs(value)
    chunks: list[int] = []
    while value:
        value, chunk = divmod(value, DECIMAL_CHUNK_BASE)
        chunks.append(chunk)
    head = str(chunks[-1])
    tail = "".join(f"{chunk:0{DECIMAL_CHUNK_DIGITS}d}" for chunk in reversed(chunks[:-1]))
    return f"{sign}{head}{tail}"


def _contiguous_result_pieces(results: list[PiBatchResult]) -> list[PiBatchResult]:
    contiguous: list[PiBatchResult] = []
    next_term = 0
    for result in sorted(results, key=lambda item: item.term_start):
        if result.term_start != next_term:
            break
        contiguous.append(result)
        next_term += result.term_count
    return contiguous


def _available_decimal_digits(request: PiComputeRequest, completed_terms: int) -> int:
    reliable_digits = completed_terms * CHUDNOVSKY_DIGITS_PER_TERM - CHUDNOVSKY_GUARD_DIGITS
    available_after_start = max(0, reliable_digits - request.start)
    return min(request.digits, available_after_start)


def _contiguous_completed_terms_from_wires(result_wires: Any) -> int:
    terms: list[tuple[int, int]] = []
    for wire in result_wires:
        if not isinstance(wire, dict) or wire.get("status") != "ok":
            continue
        terms.append((int(wire.get("term_start") or 0), int(wire.get("term_count") or 0)))
    completed_terms = 0
    for term_start, term_count in sorted(terms):
        if term_start != completed_terms:
            break
        completed_terms += term_count
    return completed_terms


def _pieces_needed_for_digits(
    request: PiComputeRequest,
    pieces: list[PiBatchResult],
    digit_end: int,
) -> list[PiBatchResult]:
    absolute_digit_end = request.start + max(0, int(digit_end))
    required_terms = max(1, math.ceil((absolute_digit_end + CHUDNOVSKY_GUARD_DIGITS) / CHUDNOVSKY_DIGITS_PER_TERM) + 1)
    selected: list[PiBatchResult] = []
    completed_terms = 0
    for piece in sorted(pieces, key=lambda item: item.term_start):
        if piece.term_start != completed_terms:
            break
        selected.append(piece)
        completed_terms += piece.term_count
        if completed_terms >= required_terms:
            break
    return selected


def _format_pi_decimal(
    p: int,
    q: int,
    t: int,
    *,
    start: int,
    digits: int,
    precision_digits: int,
) -> tuple[str, str]:
    scale = 10**precision_digits
    sqrt_value = math.isqrt(10005 * 10 ** (2 * precision_digits))
    pi_scaled = (q * 426880 * sqrt_value) // t
    integer_part = pi_scaled // scale
    fractional = _int_to_decimal_string(pi_scaled % scale).zfill(precision_digits)
    requested = fractional[start : start + digits]
    displayed_fractional = fractional[: start + digits]
    return f"{integer_part}.{displayed_fractional}", requested


def _precision_digits(request: PiComputeRequest) -> int:
    return max(1, request.start + request.digits + CHUDNOVSKY_GUARD_DIGITS)


def _terms_for_request(request: PiComputeRequest) -> int:
    return max(1, _precision_digits(request) // CHUDNOVSKY_DIGITS_PER_TERM + 1)
