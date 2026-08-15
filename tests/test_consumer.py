from channels_webtransport import AsyncWebTransportConsumer
from channels_webtransport.testing import WebTransportCommunicator


class EchoConsumer(AsyncWebTransportConsumer):
    async def connect(self) -> None:
        await self.accept(subprotocol="echo")

    async def receive_datagram(self, data: bytes) -> None:
        await self.send_datagram(b"echo:" + data)

    async def receive_stream(
        self,
        stream: int,
        data: bytes,
        end_stream: bool,
    ) -> None:
        await self.send_stream(stream, b"echo:" + data, end_stream=end_stream)


async def test_communicator_drives_datagrams_and_streams() -> None:
    communicator = WebTransportCommunicator(
        EchoConsumer.as_asgi(),
        "/wt/echo/?token=test",
        subprotocols=["echo"],
    )

    connected, subprotocol = await communicator.connect()
    assert connected is True
    assert subprotocol == "echo"

    await communicator.send_datagram(b"hello")
    assert await communicator.receive_datagram() == b"echo:hello"

    await communicator.open_stream(0)
    await communicator.send_stream(0, b"stream", end_stream=True)
    response = await communicator.receive_stream()
    assert response == {
        "type": "webtransport.stream.send",
        "stream": 0,
        "data": b"echo:stream",
        "end_stream": True,
    }

    await communicator.disconnect()


async def test_scope_contains_path_query_and_headers() -> None:
    seen_scope = None

    async def application(scope, receive, send):  # type: ignore[no-untyped-def]
        nonlocal seen_scope
        seen_scope = scope
        await receive()
        await send({"type": "webtransport.close", "status": 401})

    communicator = WebTransportCommunicator(
        application,
        "/private/?ticket=secret",
        headers=[(b"authorization", b"Bearer test")],
    )
    connected, _ = await communicator.connect()

    assert connected is False
    assert seen_scope["type"] == "webtransport"
    assert seen_scope["path"] == "/private/"
    assert seen_scope["query_string"] == b"ticket=secret"
    assert seen_scope["headers"] == [(b"authorization", b"Bearer test")]
    await communicator.wait()
