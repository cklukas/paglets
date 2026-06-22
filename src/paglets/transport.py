# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import base64
import http.client
import os
import pickle
import threading
from multiprocessing.connection import Client, Connection, Listener
from typing import Any

from .errors import HostError


PICKLE_CONTENT_TYPE = "application/x-paglets-pickle"
LOCAL_PICKLE_CHUNK_BYTES = 1024 * 1024
_LOCAL_SENDERS: list[threading.Thread] = []
_LOCAL_SENDERS_LOCK = threading.Lock()


def dump_pickle(value: Any, target: Any) -> None:
    pickle.dump(value, target, protocol=pickle.HIGHEST_PROTOCOL)


def load_pickle(source: Any) -> Any:
    return pickle.load(source)


def dump_http_chunked_pickle(connection: http.client.HTTPConnection, value: Any) -> None:
    writer = ChunkedRequestWriter(connection)
    try:
        dump_pickle(value, writer)
    finally:
        writer.close()


def load_http_pickle_payload(headers: Any, source: Any) -> dict[str, Any]:
    content_type = str(headers.get("Content-Type") or "").split(";", 1)[0].strip().casefold()
    if content_type != PICKLE_CONTENT_TYPE:
        raise HostError(f"Expected {PICKLE_CONTENT_TYPE}, got {content_type or 'missing content type'}")
    transfer_encoding = str(headers.get("Transfer-Encoding") or "").casefold()
    if "chunked" in transfer_encoding:
        stream = ChunkedRequestReader(source)
    else:
        stream = LimitedRequestReader(source, int(headers.get("Content-Length") or 0))
    payload = load_pickle(stream)
    if not isinstance(payload, dict):
        raise HostError(f"Expected pickle payload dict, got {type(payload).__name__}")
    return payload


def start_local_pickle_sender(value: Any) -> dict[str, Any]:
    authkey = os.urandom(32)
    listener = Listener(("127.0.0.1", 0), authkey=authkey)
    address = listener.address

    def serve_once() -> None:
        try:
            with listener:
                conn = listener.accept()
                try:
                    writer = LocalConnectionWriter(conn)
                    try:
                        dump_pickle(value, writer)
                    finally:
                        writer.close()
                finally:
                    conn.close()
        except Exception:
            try:
                listener.close()
            except Exception:
                pass
        finally:
            with _LOCAL_SENDERS_LOCK:
                try:
                    _LOCAL_SENDERS.remove(threading.current_thread())
                except ValueError:
                    pass

    thread = threading.Thread(target=serve_once, name="paglets-local-pickle-sender", daemon=True)
    with _LOCAL_SENDERS_LOCK:
        _LOCAL_SENDERS.append(thread)
    thread.start()
    return {
        "address": address,
        "authkey": authkey,
    }


def wait_for_local_pickle_senders(timeout: float = 5.0) -> None:
    current = threading.current_thread()
    with _LOCAL_SENDERS_LOCK:
        threads = [thread for thread in _LOCAL_SENDERS if thread is not current]
    if not threads:
        return
    per_thread_timeout = max(0.0, timeout / max(1, len(threads)))
    for thread in threads:
        thread.join(timeout=per_thread_timeout)


def receive_local_pickle(stream: dict[str, Any]) -> Any:
    address = stream.get("address")
    authkey = stream.get("authkey")
    if address is None or authkey is None:
        raise HostError("Incomplete local pickle stream metadata")
    conn = Client(address, authkey=bytes(authkey))
    try:
        return load_pickle(LocalConnectionReader(conn))
    finally:
        conn.close()


def json_safe(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"__paglets_binary__": "bytes", "base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, bytearray):
        return {"__paglets_binary__": "bytearray", "base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [json_safe(item) for item in value]
    if isinstance(value, set):
        items = [json_safe(item) for item in value]
        try:
            return sorted(items)
        except TypeError:
            return items
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    return value


def restore_binary_tag(value: Any) -> Any:
    if isinstance(value, dict) and set(value) == {"__paglets_binary__", "base64"}:
        kind = value.get("__paglets_binary__")
        raw = base64.b64decode(str(value.get("base64") or "").encode("ascii"))
        if kind == "bytearray":
            return bytearray(raw)
        if kind == "bytes":
            return raw
    return value


class ChunkedRequestWriter:
    def __init__(self, connection: http.client.HTTPConnection):
        self._connection = connection
        self._closed = False

    def write(self, data: bytes) -> int:
        if self._closed:
            raise ValueError("chunked request writer is closed")
        if not data:
            return 0
        view = memoryview(data)
        self._connection.send(f"{len(view):X}\r\n".encode("ascii"))
        self._connection.send(view)
        self._connection.send(b"\r\n")
        return len(view)

    def flush(self) -> None:
        return None

    def close(self) -> None:
        if not self._closed:
            self._connection.send(b"0\r\n\r\n")
            self._closed = True


class LimitedRequestReader:
    def __init__(self, source: Any, remaining: int):
        self._source = source
        self._remaining = max(0, int(remaining))

    def read(self, size: int = -1) -> bytes:
        if self._remaining <= 0:
            return b""
        if size is None or size < 0 or size > self._remaining:
            size = self._remaining
        data = self._source.read(size)
        self._remaining -= len(data)
        return data

    def readline(self, size: int = -1) -> bytes:
        if self._remaining <= 0:
            return b""
        if size is None or size < 0 or size > self._remaining:
            size = self._remaining
        data = self._source.readline(size)
        self._remaining -= len(data)
        return data


class ChunkedRequestReader:
    def __init__(self, source: Any):
        self._source = source
        self._buffer = bytearray()
        self._remaining = 0
        self._done = False

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            chunks = [bytes(self._buffer)]
            self._buffer.clear()
            while not self._done:
                self._read_next_chunk()
                if self._buffer:
                    chunks.append(bytes(self._buffer))
                    self._buffer.clear()
            return b"".join(chunks)
        while len(self._buffer) < size and not self._done:
            self._read_next_chunk()
        data = bytes(self._buffer[:size])
        del self._buffer[:size]
        return data

    def readline(self, size: int = -1) -> bytes:
        limit = None if size is None or size < 0 else int(size)
        while not self._done and b"\n" not in self._buffer and (limit is None or len(self._buffer) < limit):
            self._read_next_chunk()
        if limit is None:
            newline = self._buffer.find(b"\n")
            end = len(self._buffer) if newline < 0 else newline + 1
        else:
            newline = self._buffer.find(b"\n", 0, limit)
            end = min(len(self._buffer), limit) if newline < 0 else newline + 1
        data = bytes(self._buffer[:end])
        del self._buffer[:end]
        return data

    def _read_next_chunk(self) -> None:
        if self._done:
            return
        if self._remaining <= 0:
            header = self._source.readline()
            if not header:
                raise EOFError("unexpected end of chunked request")
            size_text = header.split(b";", 1)[0].strip()
            self._remaining = int(size_text, 16)
            if self._remaining == 0:
                self._read_trailers()
                self._done = True
                return
        chunk = self._source.read(self._remaining)
        if len(chunk) != self._remaining:
            raise EOFError("unexpected end of chunked request body")
        self._buffer.extend(chunk)
        self._remaining = 0
        if self._source.read(2) != b"\r\n":
            raise EOFError("malformed chunked request terminator")

    def _read_trailers(self) -> None:
        while True:
            line = self._source.readline()
            if line in (b"\r\n", b"\n", b""):
                return


class LocalConnectionWriter:
    def __init__(self, conn: Connection):
        self._conn = conn
        self._closed = False

    def write(self, data: bytes) -> int:
        if self._closed:
            raise ValueError("local pickle writer is closed")
        if not data:
            return 0
        view = memoryview(data)
        for offset in range(0, len(view), LOCAL_PICKLE_CHUNK_BYTES):
            self._conn.send_bytes(view[offset : offset + LOCAL_PICKLE_CHUNK_BYTES])
        return len(view)

    def flush(self) -> None:
        return None

    def close(self) -> None:
        if not self._closed:
            self._conn.send_bytes(b"")
            self._closed = True


class LocalConnectionReader:
    def __init__(self, conn: Connection):
        self._conn = conn
        self._buffer = bytearray()
        self._done = False

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            chunks = [bytes(self._buffer)]
            self._buffer.clear()
            while not self._done:
                self._read_next_chunk()
                if self._buffer:
                    chunks.append(bytes(self._buffer))
                    self._buffer.clear()
            return b"".join(chunks)
        while len(self._buffer) < size and not self._done:
            self._read_next_chunk()
        data = bytes(self._buffer[:size])
        del self._buffer[:size]
        return data

    def readline(self, size: int = -1) -> bytes:
        limit = None if size is None or size < 0 else int(size)
        while not self._done and b"\n" not in self._buffer and (limit is None or len(self._buffer) < limit):
            self._read_next_chunk()
        if limit is None:
            newline = self._buffer.find(b"\n")
            end = len(self._buffer) if newline < 0 else newline + 1
        else:
            newline = self._buffer.find(b"\n", 0, limit)
            end = min(len(self._buffer), limit) if newline < 0 else newline + 1
        data = bytes(self._buffer[:end])
        del self._buffer[:end]
        return data

    def _read_next_chunk(self) -> None:
        if self._done:
            return
        data = self._conn.recv_bytes()
        if not data:
            self._done = True
            return
        self._buffer.extend(data)
