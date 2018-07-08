import pytest

from aiomessaging.queues import QueueBackend, Queue

from .helpers import has_log_message

# import logging; logging.basicConfig(level=logging.DEBUG)
# logging.getLogger("pika").setLevel(logging.DEBUG)


@pytest.mark.asyncio
async def test_connect(event_loop, caplog):
    event_loop.set_debug(True)
    backend = QueueBackend()

    await backend.connect(loop=event_loop)
    await backend.channel()
    await backend.close()

    assert not has_log_message(caplog, level='ERROR')


@pytest.mark.asyncio
async def test_declare_queue(event_loop, caplog):
    event_loop.set_debug(True)
    backend = QueueBackend()
    await backend.connect(loop=event_loop)

    queue = await backend.get_queue('test_declare_exchange22')
    assert isinstance(queue, Queue)

    await backend.close()
    assert not has_log_message(caplog, level='ERROR')
