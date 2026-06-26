# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from io import BytesIO
from urllib.error import HTTPError

import pytest

from paglets.core.errors import AuthenticationError
from paglets.remote.client import HostClient


def test_missing_api_key_authentication_error_names_default_env(monkeypatch):
    def raise_unauthorized(*args, **kwargs):
        raise HTTPError(
            "https://example.test/paglets/hosts",
            401,
            "Unauthorized",
            {},
            BytesIO(b'{"error": "Authentication required", "error_type": "AuthenticationError"}'),
        )

    monkeypatch.setattr("paglets.remote.client.urlopen", raise_unauthorized)

    with pytest.raises(AuthenticationError, match="set PAGLETS_API_KEY"):
        HostClient(api_key=None).get_json("https://example.test/paglets/hosts")
