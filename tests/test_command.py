from pathlib import Path
from typing import Any

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import override_settings


async def application(
    scope: dict[str, Any],
    receive: Any,
    send: Any,
) -> None:
    del scope, receive, send


@override_settings(ASGI_APPLICATION=application, WEBTRANSPORT={})
def test_command_requires_certificate_and_key() -> None:
    with pytest.raises(CommandError, match="CERTFILE"):
        call_command("runwebtransport")


def test_command_builds_server_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    certificate = tmp_path / "certificate.pem"
    private_key = tmp_path / "key.pem"
    certificate.touch()
    private_key.touch()
    captured: dict[str, Any] = {}

    async def fake_run(self: Any) -> None:
        captured["config"] = self.config

    monkeypatch.setattr(
        "channels_webtransport.management.commands.runwebtransport.WebTransportASGIServer.run",
        fake_run,
    )

    with override_settings(
        WEBTRANSPORT={
            "APPLICATION": application,
            "HOST": "127.0.0.2",
            "PORT": 4444,
            "CERTFILE": certificate,
            "KEYFILE": private_key,
        }
    ):
        call_command("runwebtransport", verbosity=0)

    assert captured["config"].bind_host == "127.0.0.2"
    assert captured["config"].bind_port == 4444
