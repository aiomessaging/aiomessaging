"""
Message consumer tests.
"""
import asyncio
import pytest

from aiomessaging.consumers import MessageConsumer
from aiomessaging.message import Message
from aiomessaging.queues import QueueBackend
from aiomessaging.router import Router
from aiomessaging.effects import send
from aiomessaging.contrib.dummy import NullOutput
from aiomessaging.contrib.dummy.pipelines import example_pipeline

from .helpers import has_log_message, log_count


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
        available_outputs=('console', 'check', 'retry', 'never'),
        loop=event_loop
    )

    await consumer.start()

    message = Message(event_type='example_event', event_id='1')
    await queue.publish(message.to_dict())

    await asyncio.sleep(1)

    await consumer.stop()
    await backend.close()

    assert not has_log_message(caplog, level='ERROR')


@pytest.mark.asyncio
async def test_output_not_available(event_loop, caplog):
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
        available_outputs=(),
        loop=event_loop
    )

    await consumer.start()

    message = Message(event_type='example_event', event_id='1')
    await queue.publish(message.to_dict())

    await asyncio.sleep(1)

    await consumer.stop()
    await backend.close()

    # OutputNotAvailable exception must be logged
    assert log_count(caplog, level='ERROR') == 1
