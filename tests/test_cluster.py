import asyncio
import pytest

from aiomessaging.cluster import Cluster
from aiomessaging.queues import QueueBackend

from .helpers import has_log_message, log_count, send_test_message


@pytest.mark.asyncio
async def test_handle_action(event_loop, caplog):
    event_loop.set_debug(True)

    queues = QueueBackend()
    await queues.connect()

    queue = await queues.cluster_queue()
    exchange = await queues.cluster_queue()
    cluster = Cluster(queue=queue, exchange=exchange, loop=event_loop)
    await cluster.start()

    # allow switch to consume coros
    await asyncio.sleep(0.1)

    # send message and wait some time
    await send_test_message(queues.connection, queue_name=queue.name, body={
        "action": "consume",
        "queue_name": "example"
    })
    await asyncio.sleep(1)  # TODO: wait for something specific

    await cluster.stop()

    # cluster generation queue (results of "consume" action)
    gen_queue = cluster.generation_queue

    assert gen_queue.empty() is False, \
        "Generation queue must contain our 'consume' message"

    await queues.close()

    queue_name = gen_queue.get_nowait()
    assert queue_name == "example"

    assert not has_log_message(caplog, level='ERROR')


@pytest.mark.asyncio
async def test_invalid_action(event_loop, caplog):
    event_loop.set_debug(True)

    queues = QueueBackend()
    await queues.connect()

    queue = await queues.cluster_queue()
    exchange = await queues.cluster_queue()

    cluster = Cluster(queue=queue, exchange=exchange, loop=event_loop)
    await cluster.start()

    # allow switch to consume coros
    await asyncio.sleep(0.1)

    # send message and wait some time
    await send_test_message(queues.connection, queue_name=queue.name, body={
        "action": "invalid_action",
        "queue_name": "example"
    })
    await asyncio.sleep(1)

    await cluster.stop()
    await asyncio.sleep(0.1)

    # cluster generation queue (results of "consume" action)
    gen_queue = cluster.generation_queue

    assert gen_queue.empty() is True, \
        "Generation queue must not contain our *invalid* message"

    await queues.close()

    assert has_log_message(caplog, level='ERROR')
    assert log_count(caplog, level='ERROR') == 1
