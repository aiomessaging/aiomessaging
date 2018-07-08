import pytest
import asyncio

from aiomessaging.app import AiomessagingApp


def test_sync(event_loop, app):
    """Test sync App usage.

    Stop after some timeout.

    TODO: check no errors
    """
    event_loop.call_later(1, app.stop)
    app.start(loop=event_loop)


@pytest.mark.asyncio
async def test_listen_generation(event_loop, app):
    app.set_event_loop(event_loop)
    await app._start()

    #
    queue = await app.queue.generation_queue('test_listen_generation')
    await app.cluster.generation_queue.put(queue.name)

    await asyncio.sleep(1)
    await app.shutdown()


@pytest.fixture()
def app():
    return AiomessagingApp(config='tests/testing.yml')
