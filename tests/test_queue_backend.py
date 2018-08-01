import asyncio
import pytest

from aiomessaging.queues import QueueBackend, Queue

from .helpers import has_log_message

# import logging; logging.basicConfig(level=logging.DEBUG)
# logging.getLogger("pika").setLevel(logging.DEBUG)


@pytest.mark.asyncio
async def test_connect(event_loop, caplog):
    backend = QueueBackend()

    await backend.connect(loop=event_loop)
    await backend.channel()
    assert backend.is_open
    await backend.close()

    assert not has_log_message(caplog, level='ERROR')


@pytest.mark.asyncio
async def test_declare_queue(event_loop, caplog):
    backend = QueueBackend()
    await backend.connect(loop=event_loop)

    queue = await backend.get_queue('test_declare_exchange22')
    assert isinstance(queue, Queue)

    await backend.close()
    assert not has_log_message(caplog, level='ERROR')


@pytest.mark.asyncio
async def test_reconnect(event_loop, caplog):
    backend = QueueBackend(reconnect_timeout=0)
    await backend.connect(loop=event_loop)

    backend.connection.close()

    await asyncio.sleep(0.1)

    await backend.close()
    assert has_log_message(
        caplog,
        level='ERROR',
        # TODO: don't need to stick this log message in tests, bad practice
        message='Connection closed unexpectedly: 200 Normal shutdown'
    )
