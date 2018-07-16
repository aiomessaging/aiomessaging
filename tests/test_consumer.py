import asyncio
import pytest

from aiomessaging.queues import QueueBackend
from aiomessaging.consumers.base import BaseConsumer, SingleQueueConsumer

from .helpers import send_test_message, log_count


class CounterConsumerMixin:
    """Consumer with counter for tests.

    Counts received messages in `self.counter`.
    """
    counter = 0

    async def handler(self, message):
        await asyncio.sleep(0)
        self.counter += 1


@pytest.mark.asyncio
async def test_simple(event_loop, caplog):
    """Simple consumer test.

    Start consumer, send message, check that message
    succesfully delivered to handler method.
    """

    class CounterConsumer(CounterConsumerMixin, SingleQueueConsumer):
        """Counter consumer (single queue interface)
        """
        pass

    event_loop.set_debug(True)

    backend = QueueBackend(loop=event_loop)
    await backend.connect()

    connection = backend.connection
    queue = await backend.get_queue('example_queue')

    consumer = CounterConsumer(queue=queue, loop=event_loop)
    await consumer.start()

    await send_test_message(connection, "example_queue")
    await asyncio.sleep(0.1)
    await consumer.stop()
    assert consumer.counter == 1

    await backend.close()

    assert log_count(caplog, 'ERROR') == 0


@pytest.mark.asyncio
async def test_fail(event_loop, caplog):
    """Test consumer with broken _handler method.

    Consumer must log error.
    """
    event_loop.set_debug(True)

    # pylint: disable=abstract-method
    class ExampleFailHandler(SingleQueueConsumer):
        """Consumer with failing _handler.

        Always fails with Exception.
        """
        def _handler(self, queue, channel, basic_deliver, properties, body):
            # This helps to pass tests
            # self.log.error("Make it work :-)")
            raise Exception("exception from _handler")

    backend = QueueBackend(loop=event_loop)
    await backend.connect()
    queue = await backend.get_queue('aiomessaging.tests')

    consumer = ExampleFailHandler(queue=queue, loop=event_loop)
    await consumer.start()

    await send_test_message(None)

    await asyncio.sleep(0.1)

    await consumer.stop()

    assert log_count(caplog, level="ERROR") == 1

    await backend.close()


@pytest.mark.asyncio
async def test_consume_multiple_queues(event_loop, caplog):
    """Consume multiple queues and check delivery.
    """

    class CounterConsumerMultiple(CounterConsumerMixin, BaseConsumer):
        """Counter consumer (multiple queue interface).

        Count messages from all consumed queues in `self.counter`.
        """
        pass

    backend = QueueBackend(loop=event_loop)
    await backend.connect()
    queue_1 = await backend.get_queue('test_mult_1')
    queue_2 = await backend.get_queue('test_mult_2')

    consumer = CounterConsumerMultiple(loop=event_loop)
    await consumer.start()
    consumer.consume(queue_1)
    consumer.consume(queue_2)

    await send_test_message(backend.connection, "test_mult_1")
    await send_test_message(backend.connection, "test_mult_2")

    # TODO: wait for somthing specific. Pending task destroyed warns only in
    #       case of two messages (have time to switch to consumer once without
    #       sleep).
    await asyncio.sleep(0.1)

    await consumer.stop()

    assert consumer.counter == 2

    assert log_count(caplog, level="ERROR") == 0

    backend.close()
