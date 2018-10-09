import asyncio
import pytest

from aiomessaging.consumers import EventConsumer
from aiomessaging.pipeline import EventPipeline, GenerationPipeline
from aiomessaging.cluster import Cluster
from aiomessaging.queues import QueueBackend
from aiomessaging.event import Event

from .helpers import send_test_message, has_log_message, log_count


@pytest.mark.asyncio
async def test_start(event_loop: asyncio.AbstractEventLoop, caplog):
    backend = QueueBackend()
    await backend.connect()

    queue = await backend.cluster_queue()
    cluster = Cluster(queue=queue, generation_queue=asyncio.Queue(), loop=event_loop)
    await cluster.start()

    def test_callable(event):
        return event

    pipeline = EventPipeline([test_callable])
    generators = GenerationPipeline([])

    queue = await backend.events_queue('example')

    consumer = EventConsumer(
        event_type='example',
        loop=event_loop,
        event_pipeline=pipeline,
        generators=generators,
        generation_queue=asyncio.Queue(),
        queue_service=backend,
        queue=queue
    )
    await consumer.start()
    await send_test_message(
        backend.connection,
        queue_name=queue.name,
        body={'type': 'example', 'a': 1}
    )
    await asyncio.sleep(0.2)
    await consumer.stop()
    await cluster.stop()
    await backend.close()

    assert not has_log_message(caplog, level='ERROR')


@pytest.mark.asyncio
async def test_error_handler(event_loop: asyncio.AbstractEventLoop, caplog):
    backend = QueueBackend()
    await backend.connect()

    queue = await backend.cluster_queue()
    cluster = Cluster(queue=queue, generation_queue=asyncio.Queue(), loop=event_loop)
    await cluster.start()

    pipeline = EventPipeline([])
    generators = GenerationPipeline([])

    queue = await backend.events_queue('example')

    consumer = FailEventConsumer(
        event_type='example',
        loop=event_loop,
        event_pipeline=pipeline,
        generators=generators,
        generation_queue=asyncio.Queue(),
        queue_service=backend,
        queue=queue
    )
    await consumer.start()
    await send_test_message(
        backend.connection,
        queue_name=queue.name,
        body={'type': 'example', 'a': 1}
    )
    await asyncio.sleep(0.1)
    await consumer.stop()
    await cluster.stop()
    await backend.close()

    assert log_count(caplog, level='ERROR') == 1


class FailEventConsumer(EventConsumer):
    async def handle_event(self, event: Event):
        raise Exception("Test exception")
