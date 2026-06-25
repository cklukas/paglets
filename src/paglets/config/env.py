# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import os

DEFAULT_API_KEY_ENV = "PAGLETS_API_KEY"


def resolve_api_key(api_key_env: str | None = None) -> str | None:
    if api_key_env:
        api_key = os.environ.get(api_key_env)
        if not api_key:
            raise ValueError(f"--api-key-env {api_key_env!r} is not set or is empty")
        return api_key
    return os.environ.get(DEFAULT_API_KEY_ENV) or None
