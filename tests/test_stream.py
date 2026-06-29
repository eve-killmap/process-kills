import asyncio
import json

from stream import publish_invalidation


class _FakePub:
    def __init__(self):
        self.calls = []

    async def publish(self, channel, message):
        self.calls.append((channel, message))


def test_publish_invalidation_sends_targets():
    fake = _FakePub()
    asyncio.run(publish_invalidation(fake, ["system_rankings"]))
    assert len(fake.calls) == 1
    channel, message = fake.calls[0]
    assert channel == "cache:invalidate"  # default invalidate_channel
    assert json.loads(message) == {"targets": ["system_rankings"]}


def test_publish_invalidation_swallows_errors():
    class _Boom:
        async def publish(self, *a, **k):
            raise RuntimeError("redis down")

    # Must not raise.
    asyncio.run(publish_invalidation(_Boom(), ["farthest_kill"]))
