"""Base consumers.
"""
import asyncio
import logging

from typing import List
from abc import ABC, abstractmethod

import ujson

from ..queues import AbstractQueue
from ..message import Message
from ..logging import LoggerAdapter


class AbstractConsumer(ABC):

    """Abstract consumer.

    Defines public interface of consumer.
    """

    @abstractmethod
    async def start(self):
        """Start consumer.

        Used to start service tasks like monitoring etc.
        """
        pass

    @abstractmethod
    async def stop(self):
        """Stop consumer.

        Stop consume, wait already running handler tasks to complete and
        stop/cancell running service tasks.
        """
        pass

    @abstractmethod
    def consume(self, queue: AbstractQueue):
        """Start consume provided queue.
        """
        pass

    @abstractmethod
    def cancel(self, queue: AbstractQueue):
        """Stop consume provided queue.
        """
        pass


class BaseConsumer:
    """Base consumer implementation.

    Produces coroutines:

    - `self.handler` - for each reveived message
    - `self._monitoring` - monitor runned handlers and remove them if done
    """
    running = False  # determine when we shutdown gracefully
    loop: asyncio.AbstractEventLoop
    task: asyncio.Task
    monitoring_task: asyncio.Task
    consuming_queues: List[AbstractQueue]

    def __init__(self, loop=None, debug=False):
        self.loop = loop or asyncio.get_event_loop()
        self.debug = debug

        self.consuming_queues = []
        self.msg_tasks = []

        self.log = LoggerAdapter(
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
        if queue not in self.consuming_queues:
            # May be an error?
            self.log.warning("Cancel consume queue which not in consuming "
                             "queue list %s", queue.name)
        self.consuming_queues.remove(queue)
        queue.close()

    async def _monitoring(self):
        while self.running:
            await asyncio.sleep(0.1)
            for task in self.msg_tasks:
                if task.done():
                    self.msg_tasks.remove(task)
                    del task
            # TODO: GenerationConsumer._monitor_generation() pending task was
            #       destroyed warning printed to logs with shorter sleep.

    # pylint: disable=unused-argument,too-many-arguments
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
        # TODO: retry (republish), drop, explicit nack(?) handling
        await self.handler(body)
        # ack only in case of success of handler
        channel.basic_ack(delivery_tag)
        # wait for ack ok?

    # pylint: disable=unused-argument
    async def handler(self, message):
        """Queue message handler.

        Must be implemented in derived class.
        """
        raise NotImplementedError

    # pylint: disable=too-many-branches
    async def stop(self):
        """Stop consumer.

        Graceful shutdown all coroutines.
        """
        self.running = False
        self.log.info('Stop consumer')

        await asyncio.sleep(0)

        if getattr(self, 'monitoring_task', False):
            # self.monitoring_task.cancel()
            await asyncio.wait_for(self.monitoring_task, 2)
            if self.monitoring_task.done():
                self.log.info("Monitor task cancelled")
            else:
                self.log.error("Monitoring task error")

        if getattr(self, 'task', False):
            self.log.info("Graceful consumer shutdown")
            # if not self.task.done() and not self.task.cancelled():
            try:
                self.task.cancel()
                await asyncio.wait_for(self.task, 2)
            except asyncio.CancelledError:
                pass
            try:
                exc = self.task.exception()
                if exc:
                    raise exc
            except asyncio.CancelledError:
                self.log.debug('No errors, cancelled')
            except asyncio.InvalidStateError:
                self.log.debug('No errors in task')
            except Exception:  # pylint: disable=broad-except
                self.log.exception('Main task exception')
            self.log.info("Main task cancelled")

        waiting = self.msg_tasks

        for waiter in waiting:
            self.log.debug('Wait for %s', waiter)
            try:
                exc = waiter.exception()
                if exc:
                    try:
                        raise exc
                    except Exception:  # pylint: disable=broad-except
                        self.log.exception("Exception from consumer waitings")
                if waiter.cancelled() or waiter.done():
                    self.log.info('alredy done or cancelled')
                else:
                    await asyncio.wait_for(waiter, timeout=1)
            except asyncio.CancelledError:
                self.log.debug('Already cancelled')

        self.log.info('Wait for complete')


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
