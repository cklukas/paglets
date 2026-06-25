# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import pytest

from paglets.config.env import DEFAULT_API_KEY_ENV, resolve_api_key


def test_resolve_api_key_uses_default_env(monkeypatch):
    monkeypatch.setenv(DEFAULT_API_KEY_ENV, "secret")

    assert resolve_api_key() == "secret"


def test_resolve_api_key_missing_default_is_optional(monkeypatch):
    monkeypatch.delenv(DEFAULT_API_KEY_ENV, raising=False)

    assert resolve_api_key() is None


def test_resolve_api_key_empty_default_is_optional(monkeypatch):
    monkeypatch.setenv(DEFAULT_API_KEY_ENV, "")

    assert resolve_api_key() is None


def test_resolve_api_key_explicit_env_overrides_default(monkeypatch):
    monkeypatch.setenv(DEFAULT_API_KEY_ENV, "default-secret")
    monkeypatch.setenv("CUSTOM_PAGLETS_KEY", "custom-secret")

    assert resolve_api_key("CUSTOM_PAGLETS_KEY") == "custom-secret"


def test_resolve_api_key_missing_explicit_env_raises(monkeypatch):
    monkeypatch.delenv("CUSTOM_PAGLETS_KEY", raising=False)

    with pytest.raises(ValueError, match="CUSTOM_PAGLETS_KEY"):
        resolve_api_key("CUSTOM_PAGLETS_KEY")



def test_resolve_api_key_empty_explicit_env_raises(monkeypatch):
    monkeypatch.setenv("CUSTOM_PAGLETS_KEY", "")

    with pytest.raises(ValueError, match="CUSTOM_PAGLETS_KEY"):
        resolve_api_key("CUSTOM_PAGLETS_KEY")