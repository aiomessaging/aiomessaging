import pytest

from aiomessaging.consumers import GenerationConsumer
from aiomessaging.message import Message
from aiomessaging.event import Event
from aiomessaging.queues import QueueBackend


@pytest.mark.asyncio
async def test_simple(event_loop):
    backend = QueueBackend()
    await backend.connect()
    messages_queue = await backend.messages_queue('example_event')
    consumer = GenerationConsumer(
        messages_queue=messages_queue, loop=event_loop,
    )

    queue = await backend.generation_queue(name='example_queue')
    consumer.consume(queue)

    queue2 = await backend.generation_queue(name='example_queue2')
    consumer.consume(queue2)

    event = Event('example')
    message = Message(event=event)
    await consumer.handle_message(message)
    await consumer.stop()
    await backend.close()
