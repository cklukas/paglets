# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from paglets.runtime.child_endpoint import _ChildEndpoint


class _TypeErrorOnRecvConnection:
    def recv(self):
        raise TypeError("'NoneType' object cannot be interpreted as an integer")

    def send(self, message):
        del message

    def close(self):
        return None


def test_child_endpoint_reader_treats_closed_connection_type_error_as_shutdown():
    endpoint = _ChildEndpoint(_TypeErrorOnRecvConnection())

    endpoint._reader_loop()

    assert endpoint._closed.is_set()
    assert endpoint.next_request() is None
