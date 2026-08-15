import asyncio
import socket
import ssl
from pathlib import Path
from typing import Any

from pywebtransport import ClientConfig, ServerConfig, WebTransportClient
from pywebtransport.events import EventType
from pywebtransport.utils import generate_self_signed_cert

from channels_webtransport.adapter import WebTransportASGIServer


async def echo_application(scope: dict[str, Any], receive: Any, send: Any) -> None:
    assert scope["type"] == "webtransport"
    assert scope["path"] == "/echo"
    assert scope["query_string"] == b"source=loopback"
    assert (b"x-test", b"loopback") in scope["headers"]

    assert await receive() == {"type": "webtransport.connect"}
    await send({"type": "webtransport.accept", "subprotocol": "echo"})

    while True:
        message = await receive()
        if message["type"] == "webtransport.datagram.receive":
            await send({"type": "webtransport.datagram.send", "data": message["data"]})
        elif message["type"] == "webtransport.stream.receive":
            await send(
                {
                    "type": "webtransport.stream.send",
                    "stream": message["stream"],
                    "data": message["data"],
                    "end_stream": message["end_stream"],
                }
            )
        elif message["type"] == "webtransport.disconnect":
            return


async def test_real_quic_loopback(tmp_path: Path) -> None:
    generated = generate_self_signed_cert(
        hostname="localhost",
        output_dir=str(tmp_path),
        validity_days=13,
    )
    _, certificate, private_key = generated
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]

    server_config = ServerConfig(
        bind_host="127.0.0.1",
        bind_port=port,
        certfile=str(certificate),
        keyfile=str(private_key),
    )
    server = WebTransportASGIServer(echo_application, config=server_config)

    async with server:
        await server.listen()
        assert server.local_addresses[0][1] == port
        client_config = ClientConfig(
            verify_mode=ssl.CERT_NONE,
            wt_available_protocols=["echo"],
        )

        async with WebTransportClient(config=client_config) as client:
            session = await client.connect(
                url=f"https://127.0.0.1:{port}/echo?source=loopback",
                headers={"x-test": "loopback"},
                wt_available_protocols=["echo"],
            )
            assert session.wt_protocol == "echo"

            datagram_event = asyncio.create_task(
                session.events.wait_for(event_type=EventType.DATAGRAM_RECEIVED)
            )
            await asyncio.sleep(0)
            await session.send_datagram(data=b"datagram")
            event = await datagram_event
            assert event.data["data"] == b"datagram"

            stream = await session.create_bidirectional_stream()
            await stream.write_all(data=b"stream", end_stream=True)
            assert await stream.read_all() == b"stream"

            await session.close()
