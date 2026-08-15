# channels-webtransport

Experimental WebTransport support for Django Channels, backed by
[PyWebTransport](https://github.com/aiortc/pywebtransport).

This is deliberately a thin package: it translates a PyWebTransport session into a small,
versioned ASGI extension and provides a Channels-style async consumer. It is suitable for
experimentation and internal projects while the WebTransport ASGI protocol is still being
designed. It is not yet a stable public protocol.

## Install

The package requires Python 3.12 or newer.

```console
uv add channels-webtransport
```

For a local checkout:

```console
uv add --editable ../channels-webtransport
```

## Configure Django

Add the package to `INSTALLED_APPS` so Django discovers the management command:

```python
INSTALLED_APPS = [
    # ...
    "channels",
    "channels_webtransport",
]

ASGI_APPLICATION = "project.asgi.application"

WEBTRANSPORT = {
    "HOST": "127.0.0.1",
    "PORT": 4433,
    "CERTFILE": BASE_DIR / "certs" / "localhost.crt",
    "KEYFILE": BASE_DIR / "certs" / "localhost.key",
    # Any remaining PyWebTransport ServerConfig keyword arguments:
    "OPTIONS": {},
}
```

The same root ASGI application can route HTTP, WebSocket, and WebTransport scopes:

```python
# project/asgi.py
import os

from channels.routing import ProtocolTypeRouter, URLRouter
from django.core.asgi import get_asgi_application
from django.urls import path

from app.consumers import EchoWebTransportConsumer

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "project.settings")

application = ProtocolTypeRouter(
    {
        "http": get_asgi_application(),
        "webtransport": URLRouter([path("wt/echo/", EchoWebTransportConsumer.as_asgi())]),
    }
)
```

Create a consumer in the usual Channels style:

```python
# app/consumers.py
from channels_webtransport import AsyncWebTransportConsumer


class EchoWebTransportConsumer(AsyncWebTransportConsumer):
    async def receive_datagram(self, data: bytes) -> None:
        await self.send_datagram(data)

    async def receive_stream(self, stream: int, data: bytes, end_stream: bool) -> None:
        await self.send_stream(stream, data, end_stream=end_stream)
```

Then run the WebTransport listener alongside the normal HTTP/ASGI server:

```console
uv run python manage.py runwebtransport
```

`runwebtransport` owns a separate HTTP/3-over-UDP listener. Your normal Daphne/Uvicorn
process continues to serve HTTP and WebSocket traffic. Command-line flags override settings:

```console
uv run python manage.py runwebtransport \
  --host 127.0.0.1 --port 4433 \
  --certificate certs/localhost.crt --private-key certs/localhost.key
```

## Local TLS and browser testing

WebTransport requires HTTPS and HTTP/3. Generate a short-lived development certificate with
PyWebTransport:

```console
mkdir -p certs
uv run python -c \
  'from pywebtransport.utils import generate_self_signed_cert; generate_self_signed_cert(hostname="localhost", output_dir="certs", validity_days=13)'
```

For a Python client, pass `ssl.CERT_NONE` only in local tests. Browsers require the SHA-256
certificate hash in `serverCertificateHashes` for a self-signed certificate, and the certificate
must be valid for no more than 14 days. Chromium-based browsers have the broadest development
support. See [`examples/browser_echo.html`](examples/browser_echo.html) for a client.

Serve the example from localhost, paste the certificate hash into it, and open
`http://localhost:8000/browser_echo.html`:

```console
uv run python -m http.server --directory examples 8000
```

## Consumer test helper

Most consumer logic can be tested without opening a UDP socket:

```python
from channels_webtransport.testing import WebTransportCommunicator


async def test_echo():
    communicator = WebTransportCommunicator(EchoWebTransportConsumer.as_asgi(), "/wt/echo/")
    connected, _ = await communicator.connect()
    assert connected

    await communicator.send_datagram(b"hello")
    assert await communicator.receive_datagram() == b"hello"
    await communicator.disconnect()
```

The package test suite also starts a real loopback QUIC server and client:

```console
uv run pytest
```

## Experimental ASGI contract

The scope has `type="webtransport"`, ordinary HTTP request fields, and
`scope["webtransport"]["spec_version"] == "0.1"`.

Messages received by the application:

- `webtransport.connect`
- `webtransport.datagram.receive` with `data`
- `webtransport.stream.open` with `stream`, `direction`, and `initiated_by`
- `webtransport.stream.receive` with `stream`, `data`, and `end_stream`
- `webtransport.stream.reset` and `webtransport.stream.stop_sending`
- `webtransport.disconnect` with `code` and `reason`

Messages sent by the application:

- `webtransport.accept`, optionally with `subprotocol`
- `webtransport.close`, with session `code`/`reason`, or pre-accept HTTP `status`
- `webtransport.datagram.send` with `data`
- `webtransport.stream.send` with `stream`, `data`, and `end_stream`
- `webtransport.stream.reset` and `webtransport.stream.stop_receiving`

Version 0.1 supports datagrams and client-created bidirectional and unidirectional streams.
Server-created streams, a synchronous consumer, and a standardized ASGI extension are outside
this first release. HTTP middleware that expects an HTTP scope will not automatically apply;
authenticate the CONNECT request using the WebTransport scope headers or a short-lived ticket.

## Development

```console
uv sync --all-groups
uv run ruff check .
uv run mypy
uv run pytest
uv build
```

Licensed under the MIT License.
