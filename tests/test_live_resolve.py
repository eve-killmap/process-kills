import asyncio

import live


def test_process_sequence_kill_is_async():
    assert asyncio.iscoroutinefunction(live._process_sequence_kill)


def test_live_listener_accepts_esi_kwarg():
    import inspect
    params = inspect.signature(live.live_listener).parameters
    assert "esi" in params
