"""Translate PyWebTransport sessions into an experimental ASGI protocol."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Coroutine, Mapping, MutableMapping
from types import TracebackType
from typing import Any, cast
from urllib.parse import urlsplit

from pywebtransport import (
    Event,
    ServerConfig,
    WebTransportReceiveStream,
    WebTransportServer,
    WebTransportSession,
    WebTransportStream,
)
from pywebtransport.types import EventType

SPEC_VERSION = "0.1"

type ASGIScope = MutableMapping[str, Any]
type ASGIReceive = Callable[[], Awaitable[dict[str, Any]]]
type ASGISend = Callable[[dict[str, Any]], Awaitable[None]]
type ASGIApplication = Callable[[ASGIScope, ASGIReceive, ASGISend], Awaitable[None]]
type StreamHandle = WebTransportStream | WebTransportReceiveStream

logger = logging.getLogger(__name__)


def _as_bytes(value: object) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, memoryview):
        return value.tobytes()
    return str(value).encode("latin-1")


def _as_headers(headers: object) -> list[tuple[bytes, bytes]]:
    items = headers.items() if isinstance(headers, Mapping) else cast(Any, headers)
    return [(_as_bytes(name).lower(), _as_bytes(value)) for name, value in items]


def build_webtransport_scope(
    session: WebTransportSession,
    *,
    server: tuple[str, int],
) -> ASGIScope:
    """Build the versioned ASGI scope for a PyWebTransport session."""

    parsed = urlsplit(session.path)
    remote = session.remote_address
    client = (remote[0], remote[1]) if remote is not None else None

    return {
        "type": "webtransport",
        "asgi": {"version": "3.0", "spec_version": SPEC_VERSION},
        "webtransport": {"spec_version": SPEC_VERSION},
        "http_version": "3",
        "method": "CONNECT",
        "scheme": "https",
        "path": parsed.path,
        "raw_path": parsed.path.encode("utf-8"),
        "query_string": parsed.query.encode("utf-8"),
        "root_path": "",
        "headers": _as_headers(session.headers),
        "client": client,
        "server": server,
        "subprotocols": list(session.wt_available_protocols or []),
    }


class WebTransportASGIServer:
    """Run an ASGI application for every incoming WebTransport session."""

    def __init__(self, application: ASGIApplication, *, config: ServerConfig) -> None:
        self.application = application
        self.config = config
        self.server = WebTransportServer(config=config)
        self._session_tasks: set[asyncio.Task[None]] = set()
        self._entered = False
        self.server.on(event_type=EventType.SESSION_REQUEST, handler=self._session_requested)

    @property
    def local_addresses(self) -> list[tuple[str, int]]:
        """Return addresses on which the UDP server is listening."""

        return list(self.server.local_addresses)

    async def __aenter__(self) -> WebTransportASGIServer:
        await self.server.__aenter__()
        self._entered = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    async def listen(self, *, host: str | None = None, port: int | None = None) -> None:
        """Bind the underlying UDP WebTransport server."""

        await self.server.listen(host=host, port=port)

    async def serve_forever(self) -> None:
        """Wait until the server is closed."""

        await self.server.serve_forever()

    async def run(self, *, host: str | None = None, port: int | None = None) -> None:
        """Open, bind, and run the server until interrupted."""

        async with self:
            await self.listen(host=host, port=port)
            await self.serve_forever()

    async def close(self) -> None:
        """Cancel active applications and gracefully close the server."""

        self.server.off(event_type=EventType.SESSION_REQUEST, handler=self._session_requested)
        for task in self._session_tasks:
            task.cancel()
        if self._session_tasks:
            await asyncio.gather(*self._session_tasks, return_exceptions=True)
        self._session_tasks.clear()
        if self._entered:
            await self.server.close()
            self._entered = False

    def _session_requested(self, event: Event) -> None:
        data = event.data if isinstance(event.data, dict) else {}
        session = data.get("session")
        if not isinstance(session, WebTransportSession):
            logger.warning("Ignoring a session request without a session handle")
            return

        addresses = self.local_addresses
        server_address = (
            addresses[0] if addresses else (self.config.bind_host, self.config.bind_port)
        )
        bridge = _WebTransportASGISession(
            application=self.application,
            session=session,
            server_address=server_address,
            queue_size=self.config.event_queue_capacity,
        )
        task = asyncio.create_task(bridge.run(), name=f"webtransport-{session.session_id}")
        self._session_tasks.add(task)
        task.add_done_callback(self._session_tasks.discard)
        task.add_done_callback(self._report_session_failure)

    @staticmethod
    def _report_session_failure(task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.error(
                "WebTransport ASGI application failed",
                exc_info=(type(error), error, error.__traceback__),
            )


class _WebTransportASGISession:
    def __init__(
        self,
        *,
        application: ASGIApplication,
        session: WebTransportSession,
        server_address: tuple[str, int],
        queue_size: int,
    ) -> None:
        self.application = application
        self.session = session
        self.scope = build_webtransport_scope(session, server=server_address)
        self.incoming: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=queue_size)
        self.streams: dict[int, StreamHandle] = {}
        self.stream_tasks: set[asyncio.Task[None]] = set()
        self.accepted = False
        self.disconnected = False
        self.receivers_started = False

    async def run(self) -> None:
        self.session.events.on(
            event_type=EventType.DATAGRAM_RECEIVED,
            handler=self._datagram_received,
        )
        self.session.events.on(event_type=EventType.SESSION_CLOSED, handler=self._session_closed)
        await self.incoming.put({"type": "webtransport.connect"})

        try:
            await self.application(self.scope, self.receive, self.send)
        finally:
            self.session.events.off(
                event_type=EventType.DATAGRAM_RECEIVED,
                handler=self._datagram_received,
            )
            self.session.events.off(
                event_type=EventType.SESSION_CLOSED,
                handler=self._session_closed,
            )
            for task in self.stream_tasks:
                task.cancel()
            if self.stream_tasks:
                await asyncio.gather(*self.stream_tasks, return_exceptions=True)
            self.stream_tasks.clear()

            if not self.session.is_closed:
                if self.accepted:
                    await self.session.close()
                else:
                    await self.session.reject()

    async def receive(self) -> dict[str, Any]:
        return await self.incoming.get()

    async def send(self, message: dict[str, Any]) -> None:
        message_type = message.get("type")

        if message_type == "webtransport.accept":
            await self._accept(message)
        elif message_type == "webtransport.close":
            await self._close(message)
        elif message_type == "webtransport.datagram.send":
            self._require_accepted(message_type)
            await self.session.send_datagram(data=_as_bytes(message.get("data", b"")))
        elif message_type == "webtransport.stream.send":
            self._require_accepted(message_type)
            writable_stream = self._writable_stream(message)
            await writable_stream.write(
                data=_as_bytes(message.get("data", b"")),
                end_stream=bool(message.get("end_stream", False)),
            )
        elif message_type == "webtransport.stream.reset":
            self._require_accepted(message_type)
            writable_stream = self._writable_stream(message)
            await writable_stream.reset(error_code=int(message.get("code", 0)))
        elif message_type == "webtransport.stream.stop_receiving":
            self._require_accepted(message_type)
            readable_stream = self._readable_stream(message)
            await readable_stream.stop_receiving(error_code=int(message.get("code", 0)))
        else:
            raise ValueError(f"Unsupported WebTransport ASGI message: {message_type!r}")

    async def _accept(self, message: dict[str, Any]) -> None:
        if self.accepted:
            raise RuntimeError("WebTransport session has already been accepted")
        if self.session.is_closed:
            raise RuntimeError("WebTransport session is already closed")

        subprotocol = message.get("subprotocol")
        if subprotocol is not None:
            if not isinstance(subprotocol, str):
                raise TypeError("WebTransport subprotocol must be a string")
            self.session.wt_protocol = subprotocol

        await self.session.accept()
        self.accepted = True
        self._start_receivers()

    async def _close(self, message: dict[str, Any]) -> None:
        if self.session.is_closed:
            return
        if self.accepted:
            await self.session.close(
                error_code=int(message.get("code", 0)),
                reason=str(message.get("reason", "")) or None,
            )
        else:
            await self.session.reject(status_code=int(message.get("status", 403)))

    def _start_receivers(self) -> None:
        if self.receivers_started:
            return
        self.receivers_started = True
        self._create_stream_task(self._receive_bidirectional_streams(), "webtransport-bidi")
        self._create_stream_task(self._receive_unidirectional_streams(), "webtransport-uni")

    def _create_stream_task(
        self,
        coroutine: Coroutine[Any, Any, None],
        name: str,
    ) -> None:
        task: asyncio.Task[None] = asyncio.create_task(
            coroutine,
            name=f"{name}-{self.session.session_id}",
        )
        self.stream_tasks.add(task)
        task.add_done_callback(self.stream_tasks.discard)

    async def _receive_bidirectional_streams(self) -> None:
        async for stream in self.session.incoming_bidirectional_streams():
            await self._register_stream(stream, direction="bidirectional")

    async def _receive_unidirectional_streams(self) -> None:
        async for stream in self.session.incoming_unidirectional_streams():
            await self._register_stream(stream, direction="unidirectional")

    async def _register_stream(self, stream: StreamHandle, *, direction: str) -> None:
        self.streams[stream.stream_id] = stream
        stream.events.on(event_type=EventType.STREAM_CLOSED, handler=self._stream_closed)
        stream.events.on(
            event_type=EventType.STREAM_RESET_RECEIVED,
            handler=self._stream_reset,
        )
        stream.events.on(
            event_type=EventType.STOP_SENDING_RECEIVED,
            handler=self._stop_sending,
        )
        await self.incoming.put(
            {
                "type": "webtransport.stream.open",
                "stream": stream.stream_id,
                "direction": direction,
                "initiated_by": "client",
            }
        )
        self._create_stream_task(
            self._read_stream(stream, direction=direction),
            f"webtransport-stream-{stream.stream_id}",
        )

    async def _read_stream(self, stream: StreamHandle, *, direction: str) -> None:
        try:
            while True:
                data = await stream.read(max_bytes=64 * 1024)
                if not data:
                    await self.incoming.put(
                        {
                            "type": "webtransport.stream.receive",
                            "stream": stream.stream_id,
                            "data": b"",
                            "end_stream": True,
                        }
                    )
                    if direction == "unidirectional":
                        self.streams.pop(stream.stream_id, None)
                    return
                await self.incoming.put(
                    {
                        "type": "webtransport.stream.receive",
                        "stream": stream.stream_id,
                        "data": bytes(data),
                        "end_stream": False,
                    }
                )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.debug(
                "WebTransport stream reader stopped: session=%s stream=%s error=%s",
                self.session.session_id,
                stream.stream_id,
                error,
            )

    async def _datagram_received(self, event: Event) -> None:
        data = event.data if isinstance(event.data, dict) else {}
        await self.incoming.put(
            {
                "type": "webtransport.datagram.receive",
                "data": _as_bytes(data.get("data", b"")),
            }
        )

    async def _session_closed(self, event: Event) -> None:
        if self.disconnected:
            return
        self.disconnected = True
        data = event.data if isinstance(event.data, dict) else {}
        await self.incoming.put(
            {
                "type": "webtransport.disconnect",
                "code": int(data.get("error_code", 0)),
                "reason": str(data.get("reason", "") or ""),
            }
        )

    async def _stream_closed(self, event: Event) -> None:
        data = event.data if isinstance(event.data, dict) else {}
        stream_id = int(data.get("stream_id", -1))
        self.streams.pop(stream_id, None)

    async def _stream_reset(self, event: Event) -> None:
        data = event.data if isinstance(event.data, dict) else {}
        await self.incoming.put(
            {
                "type": "webtransport.stream.reset",
                "stream": int(data.get("stream_id", -1)),
                "code": int(data.get("error_code", 0)),
            }
        )

    async def _stop_sending(self, event: Event) -> None:
        data = event.data if isinstance(event.data, dict) else {}
        await self.incoming.put(
            {
                "type": "webtransport.stream.stop_sending",
                "stream": int(data.get("stream_id", -1)),
                "code": int(data.get("error_code", 0)),
            }
        )

    def _writable_stream(self, message: dict[str, Any]) -> WebTransportStream:
        stream_id = int(message["stream"])
        stream = self.streams.get(stream_id)
        if not isinstance(stream, WebTransportStream) or not stream.can_write:
            raise ValueError(f"Unknown or non-writable WebTransport stream: {stream_id}")
        return stream

    def _readable_stream(self, message: dict[str, Any]) -> StreamHandle:
        stream_id = int(message["stream"])
        stream = self.streams.get(stream_id)
        if stream is None or not stream.can_read:
            raise ValueError(f"Unknown or non-readable WebTransport stream: {stream_id}")
        return stream

    def _require_accepted(self, message_type: object) -> None:
        if not self.accepted:
            raise RuntimeError(f"Cannot send {message_type!r} before accepting the session")
        if self.session.is_closed:
            raise RuntimeError("WebTransport session is closed")
