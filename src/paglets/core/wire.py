# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from typing import Any, TypeAlias

WireValue: TypeAlias = Any
WirePayload: TypeAlias = dict[str, WireValue]
WireList: TypeAlias = list[WireValue]
