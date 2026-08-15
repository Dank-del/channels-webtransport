"""Channels consumer for the experimental WebTransport ASGI protocol."""

from __future__ import annotations

from typing import Any

from channels.consumer import AsyncConsumer
from channels.db import aclose_old_connections
from channels.exceptions import InvalidChannelLayerError, StopConsumer


class AsyncWebTransportConsumer(AsyncConsumer):
    """High-level asynchronous WebTransport consumer."""

    groups: list[str] | None = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.groups = list(self.groups or [])

    async def webtransport_connect(self, message: dict[str, Any]) -> None:
        for group in self.groups or []:
            if self.channel_layer is None:
                raise InvalidChannelLayerError(
                    "A channel layer is required when WebTransport groups are configured"
                )
            await self.channel_layer.group_add(group, self.channel_name)
        await self.connect()

    async def connect(self) -> None:
        """Accept a connection by default."""

        await self.accept()

    async def accept(self, subprotocol: str | None = None) -> None:
        message: dict[str, Any] = {"type": "webtransport.accept"}
        if subprotocol is not None:
            message["subprotocol"] = subprotocol
        await super().send(message)

    async def close(
        self,
        *,
        code: int = 0,
        reason: str = "",
        status: int = 403,
    ) -> None:
        await super().send(
            {
                "type": "webtransport.close",
                "code": code,
                "reason": reason,
                "status": status,
            }
        )

    async def webtransport_datagram_receive(self, message: dict[str, Any]) -> None:
        await self.receive_datagram(bytes(message.get("data", b"")))

    async def receive_datagram(self, data: bytes) -> None:
        """Handle an incoming unreliable datagram."""

    async def send_datagram(self, data: bytes) -> None:
        await super().send({"type": "webtransport.datagram.send", "data": data})

    async def webtransport_stream_open(self, message: dict[str, Any]) -> None:
        await self.stream_opened(
            stream=int(message["stream"]),
            direction=str(message["direction"]),
        )

    async def stream_opened(self, stream: int, direction: str) -> None:
        """Handle a client-created stream."""

    async def webtransport_stream_receive(self, message: dict[str, Any]) -> None:
        await self.receive_stream(
            stream=int(message["stream"]),
            data=bytes(message.get("data", b"")),
            end_stream=bool(message.get("end_stream", False)),
        )

    async def receive_stream(self, stream: int, data: bytes, end_stream: bool) -> None:
        """Handle bytes received on a stream."""

    async def send_stream(
        self,
        stream: int,
        data: bytes,
        *,
        end_stream: bool = False,
    ) -> None:
        await super().send(
            {
                "type": "webtransport.stream.send",
                "stream": stream,
                "data": data,
                "end_stream": end_stream,
            }
        )

    async def reset_stream(self, stream: int, *, code: int = 0) -> None:
        await super().send({"type": "webtransport.stream.reset", "stream": stream, "code": code})

    async def stop_receiving_stream(self, stream: int, *, code: int = 0) -> None:
        await super().send(
            {
                "type": "webtransport.stream.stop_receiving",
                "stream": stream,
                "code": code,
            }
        )

    async def webtransport_stream_reset(self, message: dict[str, Any]) -> None:
        await self.stream_reset(stream=int(message["stream"]), code=int(message.get("code", 0)))

    async def stream_reset(self, stream: int, code: int) -> None:
        """Handle a peer reset."""

    async def webtransport_stream_stop_sending(self, message: dict[str, Any]) -> None:
        await self.stream_stop_sending(
            stream=int(message["stream"]),
            code=int(message.get("code", 0)),
        )

    async def stream_stop_sending(self, stream: int, code: int) -> None:
        """Handle a peer request to stop sending."""

    async def webtransport_disconnect(self, message: dict[str, Any]) -> None:
        for group in self.groups or []:
            if self.channel_layer is not None:
                await self.channel_layer.group_discard(group, self.channel_name)
        await self.disconnect(
            code=int(message.get("code", 0)),
            reason=str(message.get("reason", "")),
        )
        await aclose_old_connections()
        raise StopConsumer()

    async def disconnect(self, code: int, reason: str) -> None:
        """Handle session termination."""
