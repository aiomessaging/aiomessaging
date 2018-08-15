"""Base consumers.
"""
import asyncio
import logging

from typing import List
from abc import ABC, abstractmethod

import ujson

from ..queues import AbstractQueue
from ..message import Message
from ..logging import ConsumerLoggerAdapter


MONITORING_INTERVAL = 0.1


class AbstractConsumer(ABC):

    """Abstract consumer.

    Defines public interface of consumer.
    """

    @abstractmethod
    async def start(self):
        """Start consumer.

        Used to start service tasks like monitoring etc.
        """
        pass  # pragma: no cover

    @abstractmethod
    async def stop(self):
        """Stop consumer.

        Stop consume, wait already running handler tasks to complete and
        stop/cancel running service tasks.
        """
        pass  # pragma: no cover

    @abstractmethod
    def consume(self, queue: AbstractQueue):
        """Start consume provided queue.
        """
        pass  # pragma: no cover

    @abstractmethod
    def cancel(self, queue: AbstractQueue):
        """Stop consume provided queue.
        """
        pass  # pragma: no cover


class BaseConsumer:
    """Base consumer implementation.
    """
    running = False  # determine when we shutdown gracefully
    loop: asyncio.AbstractEventLoop
    monitoring_task: asyncio.Task
    consuming_queues: List[AbstractQueue]

    def __init__(self, loop=None, debug=False):
        self.loop = loop or asyncio.get_event_loop()
        self.debug = debug

        self.consuming_queues = []
        self.msg_tasks = []

        self.log = ConsumerLoggerAdapter(
            logging.getLogger(__name__),
            {'name': self.__class__.__name__}
        )

    async def start(self):
        """Start consumer and monitoring tasks.
        """
        self.running = True
        self.monitoring_task = self.loop.create_task(self._monitoring())

    def consume(self, queue):
        """Consume coroutine.

        Create task for incoming message.
        """
        queue.consume(self._handler)
        self.consuming_queues.append(queue)

    def cancel(self, queue):
        """Cancel consume queue.
        """
        if queue not in self.consuming_queues:  # pragma: no cover
            # May be an error?
            self.log.warning("Cancel consume queue which not in consuming "
                             "queue list %s", queue.name)
        self.consuming_queues.remove(queue)
        queue.close()

    async def _monitoring(self):
        while self.running:
            # TODO: GenerationConsumer._monitor_generation() pending task was
            #       destroyed warning printed to logs with shorter sleep.
            await asyncio.sleep(MONITORING_INTERVAL)
            for task in self.msg_tasks:
                if task.done():
                    self.msg_tasks.remove(task)
                    del task

    def _handler(self, queue, channel, basic_deliver, properties, body):
        self.log.debug('Start task execution (_handler): %s', body)
        # pylint: disable=c-extension-no-member
        task = self.loop.create_task(
            self._handler_task(
                ujson.loads(body),
                channel,
                basic_deliver.delivery_tag
            )
        )
        self.msg_tasks.append(task)

    async def _handler_task(self, body, channel, delivery_tag):
        try:
            # TODO: retry (republish), drop, explicit nack(?) handling
            await self.handler(body)
            # TODO: ack only in case of success of handler
            channel.basic_ack(delivery_tag)
            # TODO: wait for ack ok
        # pylint: disable=broad-except
        except Exception:  # pragma: no cover
            self.log.exception("Error in handler task")

    async def handler(self, message):
        """Queue message handler.

        Must be implemented in derived class.
        """
        raise NotImplementedError  # pragma: no cover

    # pylint: disable=too-many-branches
    async def stop(self):
        """Stop consumer.

        Graceful shutdown all coroutines.
        """
        self.running = False
        self.log.info('Stop consumer')

        for queue in self.consuming_queues:
            self.cancel(queue)

        await asyncio.sleep(0)

        if getattr(self, 'monitoring_task', False):
            await asyncio.wait_for(self.monitoring_task, 2)

        self.log.debug('Stopped.')


# pylint: disable=abstract-method
class SingleQueueConsumer(BaseConsumer):
    """Single queue consumer.
    """
    def __init__(self, queue, **kwargs):
        super().__init__(**kwargs)
        self.queue = queue

    async def start(self):
        await super().start()
        self.consume_default()

    def consume_default(self):
        """Consume default queue.
        """
        self.consume(self.queue)


class MessageConsumerMixIn:
    """Message consumer mixin.
    """
    async def handler(self, message):
        """Internal message handler.

        Converts queue message to Message instance.
        """
        obj = Message.from_dict(message)
        await self.handle_message(obj)

    async def handle_message(self, message: Message):
        """Message handler.

        Must be defined on derived class.
        """
        raise NotImplementedError  # pragma: no cover


class BaseMessageConsumer(MessageConsumerMixIn, SingleQueueConsumer):

    """Message consumer.

    Base consumer class, transforms amqp message to internal Message object.
    """

    pass
