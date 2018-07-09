import asyncio
import pytest

from aiomessaging.queues import QueueBackend
from aiomessaging.consumers.base import SingleQueueConsumer

from .helpers import has_log_message, send_test_message, log_count


@pytest.mark.asyncio
async def test_simple(event_loop, caplog):
    """Simple consumer test.

    Start consumer, send message, check that message
    succesfully delivered to handler method.
    """
    event_loop.set_debug(True)

    class ExampleConsumer(SingleQueueConsumer):
        """Consumer with counter.

        Counts received messages in `self.counter`.
        """
        queue_name = 'aiomessaging.tests'
        counter = 0

        async def handler(self, message):
            print("Handler")
            await asyncio.sleep(0)
            # message.ack()
            self.counter += 1

    backend = QueueBackend(loop=event_loop)
    await backend.connect()

    connection = backend.connection
    queue = await backend.get_queue('example_queue')

    consumer = ExampleConsumer(queue=queue, loop=event_loop)
    await consumer.start()

    await send_test_message(connection, "example_queue")
    # allow message to be consumed
    # TODO: wait for somthing specific
    await asyncio.sleep(1)

    await consumer.stop()
    assert consumer.counter == 1

    await backend.close()

    assert log_count(caplog, 'ERROR') == 0


@pytest.mark.asyncio
async def test_fail(event_loop, caplog):
    """Test consumer with broken _handler method.

    Consumer must log error.
    """
    # pylint: disable=abstract-method
    class ExampleFailHandler(SingleQueueConsumer):
        """Consumer with failing _handler.

        Always fails with Exception.
        """
        def _handler(self, channel, basic_deliver, properties, body):
            raise Exception("exception from _handler")

    backend = QueueBackend(loop=event_loop)
    await backend.connect()
    queue = await backend.get_queue('aiomessaging.tests')

    consumer = ExampleFailHandler(queue=queue, loop=event_loop)
    await consumer.start()

    await send_test_message(None)
    # allow message to be consumed
    # TODO: wait for somthing specific
    await asyncio.sleep(0.1)

    await consumer.stop()

    assert has_log_message(caplog, "exception from _handler", "ERROR")
    assert log_count(caplog, level="ERROR") == 1

    await backend.close()
