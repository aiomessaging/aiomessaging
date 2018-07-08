import pytest

from aiomessaging.consumers import GenerationConsumer
from aiomessaging.message import Message
from aiomessaging.event import Event
from aiomessaging.queues import QueueBackend


@pytest.mark.asyncio
async def test_simple(event_loop):
    backend = QueueBackend()
    await backend.connect()
    queue = await backend.generation_queue(name='example_queue')
    messages_queue = await backend.messages_queue('example_event')
    consumer = GenerationConsumer(
        queue=queue, messages_queue=messages_queue, loop=event_loop,
    )
    event = Event('example')
    message = Message(event=event)
    await consumer.handle_message(message)
    await consumer.stop()
    await backend.close()
