"""
Application object tests.
"""
import asyncio
from unittest import mock

import pytest

from aiomessaging.app import AiomessagingApp


def test_sync(event_loop, app):
    """Test sync App usage.

    Stop after some timeout.

    TODO: check no errors
    """
    event_loop.call_later(0.5, app.stop)
    app.start(loop=event_loop)


@pytest.mark.asyncio
async def test_listen_generation(event_loop, app):
    await app.send('example_event', {})

    app.set_event_loop(event_loop)
    await app._start()

    await app.send('example_event', {})

    await asyncio.sleep(0.1)

    await app.shutdown()

@pytest.mark.asyncio
async def test_default_config():
    with mock.patch('aiomessaging.app.apply_logging_configuration'):
        AiomessagingApp()


@pytest.fixture()
def app():
    """App fixture with testing config.
    """
    with mock.patch('aiomessaging.app.apply_logging_configuration'):
        return AiomessagingApp(config='tests/testing.yml')
