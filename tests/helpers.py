"""
tests helpers
"""
import logging
import asyncio

from unittest.mock import Mock

from aiomessaging.queues import QueueBackend
from aiomessaging.consumers import OutputConsumer, MessageConsumer
from aiomessaging.router import Router

logging.getLogger("pika").setLevel(logging.INFO)
logging.getLogger("aio_pika").setLevel(logging.INFO)


class OutputConsumerContext:

    """OutputConsumer async context manager.

    Provide ready to use output consumer instance with defined pipeline.
    """

    def __init__(self, backend, output, pipeline):
        self.backend = backend
        self.pipeline = pipeline
        self.output = output
        self.output_consumer = None

    async def __aenter__(self):
        queue = await self.backend.output_queue('example_event', self.output)
        messages_queue = await self.backend.messages_queue('example_event')
        router = Router(self.pipeline)
        self.output_consumer = OutputConsumer(
            router=router,
            event_type='example_event',
            messages_queue=messages_queue,
            queue=queue
        )
        await self.output_consumer.start()
        return self.output_consumer

    async def __aexit__(self, exc_type, exc, tb):
        await self.output_consumer.stop()


class MessageConsumerContext:

    """MessageConsumer context manager.

    Provide ready to use message consumer instance with defined pipeline.
    """

    def __init__(self, backend, pipeline):
        self.backend = backend
        self.pipeline = pipeline
        self.message_consumer = None

    async def __aenter__(self):
        queue = await self.backend.messages_queue('example_event')
        output_queue = await self.backend.output_queue('example_event')
        router = Router(self.pipeline)
        self.message_consumer = MessageConsumer(
            event_type='example_event',
            router=router,
            output_queue=output_queue,
            queue=queue,
        )
        await self.message_consumer.start()
        return self.message_consumer

    async def __aexit__(self, exc_type, exc, tb):
        await self.message_consumer.stop()


async def send_test_message(connection, queue_name="aiomessaging.tests",
                            body=None):
    backend = QueueBackend()
    await backend.connect()
    if body is None:
        body = {
            "event_id": "123",
            "event_type": "example_event"
        }
    queue = await backend.get_queue(
        exchange='',
        exchange_type='direct',
        routing_key=queue_name
    )
    await queue.publish(body=body, routing_key=queue_name)


def has_log_message(caplog, message=None, level=None):
    """Check caplog contains log message.
    """
    for r in caplog.records:
        if level and r.levelname != level:
            continue
        if not message or message in r.getMessage() or message in r.exc_text:
            return True
    return False


def log_count(caplog, message=None, level=None):
    result = 0
    for r in caplog.records:
        if level and r.levelname == level:
            result += 1
            continue
        if message and message in r.getMessage():
            result += 1
    return result


async def wait_messages(consumer, count=1):
    """Wait for specified amount of messages to be handled by consumer.
    """
    for _ in range(count):
        await consumer.last_messages.get()


def mock_coro(return_value=None):
    @asyncio.coroutine
    def mock_coro(*args, **kwargs):
        return return_value

    return Mock(wraps=mock_coro)
