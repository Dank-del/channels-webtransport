"""Run the project's ASGI application on a WebTransport UDP listener."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.utils.module_loading import import_string
from pywebtransport import ServerConfig

from channels_webtransport.adapter import ASGIApplication, WebTransportASGIServer


class Command(BaseCommand):
    help = "Run the configured ASGI application on a WebTransport UDP listener"

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--host", help="UDP bind address")
        parser.add_argument("--port", type=int, help="UDP bind port")
        parser.add_argument("--certificate", dest="certfile", help="TLS certificate path")
        parser.add_argument("--private-key", dest="keyfile", help="TLS private key path")

    def handle(self, *args: Any, **options: Any) -> None:
        raw_settings = getattr(settings, "WEBTRANSPORT", {})
        if not isinstance(raw_settings, Mapping):
            raise CommandError("WEBTRANSPORT must be a mapping")

        application = self._load_application(raw_settings.get("APPLICATION"))
        host = options.get("host") or raw_settings.get("HOST", "127.0.0.1")
        port = options.get("port") or raw_settings.get("PORT", 4433)
        certfile = options.get("certfile") or raw_settings.get("CERTFILE")
        keyfile = options.get("keyfile") or raw_settings.get("KEYFILE")

        if not certfile or not keyfile:
            raise CommandError(
                "Configure WEBTRANSPORT['CERTFILE'] and WEBTRANSPORT['KEYFILE'], "
                "or pass --certificate and --private-key"
            )
        for label, value in (("certificate", certfile), ("private key", keyfile)):
            if not Path(str(value)).is_file():
                raise CommandError(f"WebTransport {label} does not exist: {value}")

        raw_server_options = raw_settings.get("OPTIONS", {})
        if not isinstance(raw_server_options, Mapping):
            raise CommandError("WEBTRANSPORT['OPTIONS'] must be a mapping")
        server_options = dict(raw_server_options)
        server_options.update(
            bind_host=str(host),
            bind_port=int(port),
            certfile=str(certfile),
            keyfile=str(keyfile),
        )
        config = ServerConfig(**server_options)
        server = WebTransportASGIServer(application, config=config)

        self.stdout.write(self.style.SUCCESS(f"Starting WebTransport on udp://{host}:{port}"))
        asyncio.run(server.run())

    @staticmethod
    def _load_application(configured: object) -> ASGIApplication:
        application: object = configured or getattr(settings, "ASGI_APPLICATION", None)
        if application is None:
            raise CommandError(
                "Set ASGI_APPLICATION or WEBTRANSPORT['APPLICATION'] to an ASGI application"
            )
        if isinstance(application, str):
            application = import_string(application)
        if not callable(application):
            raise CommandError("The configured WebTransport ASGI application is not callable")
        return cast(ASGIApplication, application)
