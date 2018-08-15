import asyncio
import pytest

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

    queue = await app.queue.generation_queue('test_listen_generation')
    await app.cluster.generation_queue.put(queue.name)

    await app.shutdown()

    # TODO: this timeout required to prevent asyncio warnings looks like we
    #       need to wait something specific in `app.shutdown()``
    #       (`GenerationConsumer._monitor_generation`)
    await asyncio.sleep(0.5)


@pytest.fixture()
def app():
    """App fixture with testing config.
    """
    return AiomessagingApp(config='tests/testing.yml')
