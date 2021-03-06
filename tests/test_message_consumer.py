"""
Message consumer tests.
"""
import asyncio
import pytest

from aiomessaging.consumers import MessageConsumer
from aiomessaging.message import Message
from aiomessaging.queues import QueueBackend
from aiomessaging.router import Router
from aiomessaging.contrib.dummy.pipelines import example_pipeline

from .helpers import (
    has_log_message,
    log_count,
    wait_messages,
)


@pytest.mark.asyncio
async def test_simple(event_loop, caplog):
    """Send message to output backend and check no errors happens.
    """
    backend = QueueBackend()
    await backend.connect()
    output_queue = await backend.output_queue('example_event', 'sns')
    router = Router(output_pipeline=example_pipeline)
    queue = await backend.messages_queue('example_event')
    consumer = MessageConsumer(
        event_type='example_event',
        router=router,
        output_queue=output_queue,
        queue=queue,
        loop=event_loop
    )

    await consumer.start()

    message = Message(event_type='example_event', event_id='1')
    await queue.publish(message.to_dict())

    await wait_messages(consumer)

    await consumer.stop()
    await backend.close()

    assert not has_log_message(caplog, level='ERROR')


@pytest.mark.asyncio
async def test_any_output_available(event_loop, caplog):
    backend = QueueBackend()
    await backend.connect()
    output_queue = await backend.output_queue('example_event', 'sns')
    router = Router(output_pipeline=example_pipeline)
    queue = await backend.messages_queue('example_event')
    consumer = MessageConsumer(
        event_type='example_event',
        router=router,
        output_queue=output_queue,
        queue=queue,
        loop=event_loop
    )

    await consumer.start()

    message = Message(event_type='example_event', event_id='1')
    await queue.publish(message.to_dict())

    await wait_messages(consumer)

    await consumer.stop()
    await backend.close()

    # Skip output logs
    assert log_count(caplog, level='ERROR') == 0
