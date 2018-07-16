import asyncio
import pytest

from aiomessaging.consumers import GenerationConsumer
from aiomessaging.message import Message
from aiomessaging.event import Event
from aiomessaging.queues import QueueBackend

from .helpers import send_test_message


@pytest.mark.asyncio
async def test_simple(event_loop):
    backend = QueueBackend()
    await backend.connect()
    messages_queue = await backend.messages_queue('example_event')
    consumer = GenerationConsumer(
        messages_queue=messages_queue, loop=event_loop,
        cleanup_timeout=0
    )
    await consumer.start()

    queue = await backend.generation_queue(name='example_queue')
    consumer.consume(queue)

    queue2 = await backend.generation_queue(name='example_queue2')
    consumer.consume(queue2)

    await send_test_message(
        backend.connection,
        queue_name=queue.name,
        body={'type': 'example', 'a': 1}
    )

    event = Event('example')
    message = Message(event_type=event.type, event_id=event.id)
    await consumer.handle_message(message)

    await asyncio.sleep(1)
    await consumer.stop()
    await backend.close()
