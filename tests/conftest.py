import django
from django.conf import settings

if not settings.configured:
    settings.configure(
        ASGI_APPLICATION="tests.test_command.application",
        CHANNEL_LAYERS={"default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}},
        INSTALLED_APPS=["channels_webtransport"],
        SECRET_KEY="test-secret",
    )

django.setup()
