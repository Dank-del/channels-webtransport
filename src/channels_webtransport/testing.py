"""In-memory testing helpers for WebTransport consumers."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit

from asgiref.testing import ApplicationCommunicator

from .adapter import SPEC_VERSION, ASGIApplication


class WebTransportCommunicator(ApplicationCommunicator):
    """Drive a WebTransport ASGI application without opening a QUIC socket."""

    def __init__(
        self,
        application: ASGIApplication,
        path: str,
        *,
        headers: list[tuple[bytes, bytes]] | None = None,
        subprotocols: list[str] | None = None,
    ) -> None:
        parsed = urlsplit(path)
        scope = {
            "type": "webtransport",
            "asgi": {"version": "3.0", "spec_version": SPEC_VERSION},
            "webtransport": {"spec_version": SPEC_VERSION},
            "http_version": "3",
            "method": "CONNECT",
            "scheme": "https",
            "path": parsed.path,
            "raw_path": parsed.path.encode(),
            "query_string": parsed.query.encode(),
            "root_path": "",
            "headers": headers or [],
            "client": ("127.0.0.1", 12345),
            "server": ("127.0.0.1", 4433),
            "subprotocols": subprotocols or [],
        }
        super().__init__(application, scope)

    async def connect(self, timeout: float = 1) -> tuple[bool, str | None]:
        await self.send_input({"type": "webtransport.connect"})
        response = await self.receive_output(timeout)
        if response["type"] == "webtransport.accept":
            return True, response.get("subprotocol")
        if response["type"] == "webtransport.close":
            return False, None
        raise AssertionError(f"Unexpected WebTransport connection response: {response!r}")

    async def send_datagram(self, data: bytes) -> None:
        await self.send_input({"type": "webtransport.datagram.receive", "data": data})

    async def receive_datagram(self, timeout: float = 1) -> bytes:
        response = await self.receive_output(timeout)
        if response["type"] != "webtransport.datagram.send":
            raise AssertionError(f"Expected a datagram, received: {response!r}")
        return bytes(response["data"])

    async def open_stream(self, stream: int, *, direction: str = "bidirectional") -> None:
        await self.send_input(
            {
                "type": "webtransport.stream.open",
                "stream": stream,
                "direction": direction,
                "initiated_by": "client",
            }
        )

    async def send_stream(
        self,
        stream: int,
        data: bytes,
        *,
        end_stream: bool = False,
    ) -> None:
        await self.send_input(
            {
                "type": "webtransport.stream.receive",
                "stream": stream,
                "data": data,
                "end_stream": end_stream,
            }
        )

    async def receive_stream(self, timeout: float = 1) -> dict[str, Any]:
        response = await self.receive_output(timeout)
        if response["type"] != "webtransport.stream.send":
            raise AssertionError(f"Expected stream data, received: {response!r}")
        return response

    async def disconnect(self, *, code: int = 0, reason: str = "") -> None:
        await self.send_input({"type": "webtransport.disconnect", "code": code, "reason": reason})
        await self.wait(timeout=1)
