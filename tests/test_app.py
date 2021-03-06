"""
Application object tests.
"""
from unittest import mock

import pytest

from aiomessaging.app import AiomessagingApp

from .helpers import wait_messages


def test_sync(event_loop, app):
    """Test sync App usage.

    Stop after some timeout.

    TODO: check no errors
    """
    event_loop.call_later(0.5, app.stop)
    app.start(loop=event_loop)


@pytest.mark.asyncio
async def test_listen_generation(event_loop, app):
    event_type = 'example_event'
    await app.send(event_type, {})

    app.set_event_loop(event_loop)
    await app._start()

    await app.send(event_type, {})

    await wait_messages(app.consumers.message_consumers[event_type], 2)

    await app.shutdown()


@pytest.fixture()
def app():
    """App fixture with testing config.
    """
    with mock.patch('aiomessaging.app.apply_logging_configuration'):
        return AiomessagingApp(config='tests/testing.yml')
