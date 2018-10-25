import pytest

from aiomessaging.cluster import Cluster
from aiomessaging.queues import QueueBackend

from .helpers import (
    has_log_message,
    log_count,
    send_test_message,
    wait_messages,
    mock_coro,
)


@pytest.mark.asyncio
async def test_handle_action(event_loop, caplog):
    event_loop.set_debug(True)

    queues = QueueBackend()
    await queues.connect()

    queue = await queues.cluster_queue()
    start_consume_handler = mock_coro()
    cluster = Cluster(queue=queue, loop=event_loop)
    cluster.on_start_consume(start_consume_handler)
    await cluster.start()

    # send message and wait some time
    await send_test_message(queues.connection, queue_name=queue.name, body={
        "action": Cluster.START_CONSUME,
        "queue_name": "example"
    })
    await wait_messages(cluster)

    await cluster.stop()

    start_consume_handler.assert_called_once_with(queue_name='example')

    await queues.close()

    assert not has_log_message(caplog, level='ERROR')


@pytest.mark.asyncio
async def test_invalid_action(event_loop, caplog):
    event_loop.set_debug(True)

    queues = QueueBackend()
    await queues.connect()

    queue = await queues.cluster_queue()
    start_consume_handler = mock_coro()

    cluster = Cluster(queue=queue, loop=event_loop)
    cluster.on_start_consume(start_consume_handler)
    await cluster.start()

    # send message and wait some time
    await send_test_message(queues.connection, queue_name=queue.name, body={
        "action": "invalid_action",
        "queue_name": "example"
    })
    await send_test_message(queues.connection, queue_name=queue.name, body={
        "action": Cluster.START_CONSUME,
    })
    await send_test_message(queues.connection, queue_name=queue.name, body={
        "queue_name": "example"
    })

    await wait_messages(cluster, 3)

    await cluster.stop()

    # second action called with no arguments
    start_consume_handler.assert_called_once_with()

    await queues.close()

    # errors from first and third invalid actions
    assert has_log_message(caplog, level='ERROR')
    assert log_count(caplog, level='ERROR') == 2


@pytest.mark.asyncio
async def test_start_consume(event_loop, caplog):
    queues = QueueBackend()
    await queues.connect()

    queue = await queues.cluster_queue()

    cluster = Cluster(queue=queue, loop=event_loop)
    await cluster.start()

    await cluster.start_consume('example')

    await wait_messages(cluster)

    await cluster.stop()
    await queues.close()

    assert not has_log_message(caplog, level='ERROR')
