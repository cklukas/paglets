# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

import base64
import contextlib
import http.client
import pickle
import threading
import uuid
from multiprocessing import resource_tracker, shared_memory
from typing import Any

from paglets.core.errors import HostError

PICKLE_CONTENT_TYPE = "application/x-paglets-pickle"
LOCAL_PICKLE_CHUNK_BYTES = 1024 * 1024
LOCAL_PICKLE_SEGMENT_BYTES = 64 * 1024 * 1024
_LOCAL_PICKLE_STREAMS: dict[str, list[shared_memory.SharedMemory]] = {}
_LOCAL_PICKLE_STREAMS_LOCK = threading.Lock()


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
    writer = SharedMemoryPickleWriter(segment_size=LOCAL_PICKLE_SEGMENT_BYTES)
    try:
        dump_pickle(value, writer)
        return writer.finish()
    except Exception:
        writer.abort()
        raise


def wait_for_local_pickle_senders(timeout: float = 5.0) -> None:
    return None


def release_local_pickle_sender(stream_or_token: dict[str, Any] | str) -> None:
    token = stream_or_token if isinstance(stream_or_token, str) else str(stream_or_token.get("token") or "")
    if not token:
        return
    with _LOCAL_PICKLE_STREAMS_LOCK:
        handles = _LOCAL_PICKLE_STREAMS.pop(token, [])
    for handle in handles:
        with contextlib.suppress(Exception):
            handle.close()


def receive_local_pickle(stream: dict[str, Any]) -> Any:
    if stream.get("kind") != "shared_memory_pickle":
        raise HostError("Unsupported local pickle stream metadata")
    reader = SharedMemoryPickleReader(stream)
    try:
        return load_pickle(reader)
    finally:
        reader.close(unlink=True)
        release_local_pickle_sender(stream)


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


class SharedMemoryPickleWriter:
    def __init__(self, *, segment_size: int):
        self._segment_size = max(1, int(segment_size))
        self._token = uuid.uuid4().hex[:12]
        self._segments: list[dict[str, Any]] = []
        self._handles: list[shared_memory.SharedMemory] = []
        self._current: shared_memory.SharedMemory | None = None
        self._current_used = 0
        self._total_size = 0
        self._closed = False

    def write(self, data: bytes) -> int:
        if self._closed:
            raise ValueError("shared-memory pickle writer is closed")
        if not data:
            return 0
        view = memoryview(data)
        written = 0
        while written < len(view):
            if self._current is None or self._current_used >= self._segment_size:
                self._open_segment()
            assert self._current is not None
            available = self._segment_size - self._current_used
            size = min(available, len(view) - written)
            self._current.buf[self._current_used : self._current_used + size] = view[written : written + size]
            self._current_used += size
            self._segments[-1]["used"] = self._current_used
            self._total_size += size
            written += size
        return len(view)

    def flush(self) -> None:
        return None

    def finish(self) -> dict[str, Any]:
        metadata = {
            "kind": "shared_memory_pickle",
            "token": self._token,
            "segment_size": self._segment_size,
            "total_size": self._total_size,
            "segments": [dict(segment) for segment in self._segments],
        }
        if not self._closed:
            self._closed = True
            with _LOCAL_PICKLE_STREAMS_LOCK:
                _LOCAL_PICKLE_STREAMS[self._token] = list(self._handles)
            self._handles = []
            self._current = None
        return metadata

    def abort(self) -> None:
        self._closed = True
        with _LOCAL_PICKLE_STREAMS_LOCK:
            registered = _LOCAL_PICKLE_STREAMS.pop(self._token, [])
        self._handles.extend(registered)
        for handle in self._handles:
            try:
                handle.unlink()
            except FileNotFoundError:
                pass
            except Exception:
                pass
        self._close_handles()

    def _open_segment(self) -> None:
        name = f"pgl{self._token}{len(self._segments):x}"
        handle = shared_memory.SharedMemory(name=name, create=True, size=self._segment_size)
        _unregister_shared_memory(handle)
        self._handles.append(handle)
        self._current = handle
        self._current_used = 0
        self._segments.append({"name": handle.name, "size": self._segment_size, "used": 0})

    def _close_handles(self) -> None:
        for handle in self._handles:
            with contextlib.suppress(Exception):
                handle.close()
        self._handles = []
        self._current = None

    def close(self) -> None:
        if not self._closed:
            self.finish()


class SharedMemoryPickleReader:
    def __init__(self, stream: dict[str, Any]):
        self._segments_meta = [dict(segment) for segment in stream.get("segments") or []]
        self._handles: list[shared_memory.SharedMemory] = []
        self._index = 0
        self._offset = 0
        self._closed = False
        try:
            self._handles = [
                shared_memory.SharedMemory(name=str(segment["name"]), create=False) for segment in self._segments_meta
            ]
        except Exception:
            self.close(unlink=False)
            raise

    def read(self, size: int = -1) -> bytes:
        if self._closed:
            raise ValueError("shared-memory pickle reader is closed")
        if size is None or size < 0:
            chunks: list[bytes] = []
            while True:
                chunk = self.read(LOCAL_PICKLE_CHUNK_BYTES)
                if not chunk:
                    return b"".join(chunks)
                chunks.append(chunk)
        remaining = int(size)
        chunks: list[bytes] = []
        while remaining > 0 and self._index < len(self._handles):
            used = int(self._segments_meta[self._index].get("used") or 0)
            if self._offset >= used:
                self._index += 1
                self._offset = 0
                continue
            amount = min(remaining, used - self._offset)
            handle = self._handles[self._index]
            chunks.append(bytes(handle.buf[self._offset : self._offset + amount]))
            self._offset += amount
            remaining -= amount
        return b"".join(chunks)

    def readline(self, size: int = -1) -> bytes:
        limit = None if size is None or size < 0 else int(size)
        chunks: list[bytes] = []
        while limit is None or sum(len(chunk) for chunk in chunks) < limit:
            remaining = 1 if limit is None else max(0, limit - sum(len(chunk) for chunk in chunks))
            if remaining <= 0:
                break
            chunk = self.read(1)
            if not chunk:
                break
            chunks.append(chunk)
            if chunk == b"\n":
                break
        return b"".join(chunks)

    def close(self, *, unlink: bool = False) -> None:
        if self._closed:
            return
        for handle in self._handles:
            if unlink:
                try:
                    handle.unlink()
                except FileNotFoundError:
                    pass
                except Exception:
                    pass
            with contextlib.suppress(Exception):
                handle.close()
        self._handles = []
        self._closed = True


def _unregister_shared_memory(handle: shared_memory.SharedMemory) -> None:
    with contextlib.suppress(Exception):
        resource_tracker.unregister(handle._name, "shared_memory")
