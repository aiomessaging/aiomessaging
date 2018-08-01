import asyncio
import pytest

from aiomessaging import QueueBackend
from aiomessaging.router import Router
from aiomessaging.consumers.output import OutputConsumer

from .helpers import send_test_message, log_count
from .tmp import sequence_pipeline


@pytest.mark.asyncio
async def test_output_consumer_handler(event_loop, caplog):
    backend = QueueBackend()
    await backend.connect()
    queue = await backend.output_queue('example_event', 'sns')
    messages_queue = await backend.messages_queue('example_event')
    router = Router(sequence_pipeline)
    output_consumer = OutputConsumer(
        router=router,
        event_type='example_event',
        messages_queue=messages_queue,
        queue=queue,
        loop=event_loop
    )
    await output_consumer.start()
    await send_test_message(backend.connection, "test_mult_1")
    await asyncio.sleep(1)
    assert log_count(caplog, level='ERROR') == 0
    await output_consumer.stop()
    await backend.close()
