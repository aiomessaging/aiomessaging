import asyncio
import pytest

from aiomessaging.consumers import EventConsumer
from aiomessaging.pipeline import EventPipeline, GenerationPipeline
from aiomessaging.cluster import Cluster
from aiomessaging.queues import QueueBackend

from .helpers import send_test_message, has_log_message


@pytest.mark.asyncio
async def test_start(event_loop: asyncio.AbstractEventLoop, caplog):
    backend = QueueBackend()
    await backend.connect()

    queue = await backend.cluster_queue()
    exchange = await backend.cluster_queue()
    cluster = Cluster(queue=queue, exchange=exchange, loop=event_loop)
    await cluster.start()

    pipeline = EventPipeline([])
    generators = GenerationPipeline([])

    queue = await backend.events_queue('example')

    consumer = EventConsumer(
        event_type='example',
        loop=event_loop,
        event_pipeline=pipeline,
        generators=generators,
        cluster=cluster,
        queue_service=backend,
        queue=queue
    )
    await consumer.start()
    await send_test_message(
        backend.connection,
        queue_name='aiomessaging.tests.event_consumer.example',
        body={'type': 'example', 'a': 1}
    )
    await consumer.stop()
    await cluster.stop()
    await backend.close()

    assert not has_log_message(caplog, level='ERROR')
