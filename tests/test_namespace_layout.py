# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import importlib

import pytest


def test_flat_module_imports_are_removed() -> None:
    for module_name in ("paglets.host", "paglets.messages", "paglets.admin", "paglets.serde"):
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module(module_name)


def test_root_package_has_no_convenience_exports() -> None:
    with pytest.raises(ImportError):
        exec("from paglets import Host", {})


def test_topic_module_imports_are_available() -> None:
    from paglets.core.messages import Message
    from paglets.remote.admin import PagletsAdminClient
    from paglets.runtime.host import Host
    from paglets.serialization.serde import dataclass_to_wire

    assert Host.__name__ == "Host"
    assert Message.__name__ == "Message"
    assert PagletsAdminClient.__name__ == "PagletsAdminClient"
    assert callable(dataclass_to_wire)
