import asyncio
import pytest

from aiomessaging.consumers import GenerationConsumer
from aiomessaging.message import Message
from aiomessaging.event import Event
from aiomessaging.queues import QueueBackend

from .helpers import send_test_message, has_log_message


@pytest.mark.asyncio
async def test_simple(event_loop, caplog):
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

    event = Event('example_event')
    message = Message(event_type=event.type, event_id=event.id)

    await send_test_message(
        backend.connection,
        queue_name=queue.name,
        body=message.to_dict()
    )

    await consumer.last_messages.get()
    # TODO: replace with a separate and specific test (tmp queue deletion)
    # wait tmp queue deletion
    await asyncio.sleep(0.1)

    await consumer.stop()
    await backend.close()

    assert not has_log_message(caplog, level='ERROR')
