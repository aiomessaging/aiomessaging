"""Base consumers.
"""
import asyncio
import logging

import ujson

from ..message import Message
from ..logging import LoggerAdapter


class BaseConsumer:
    """Base consumer implementation.

    Produces coroutines:

    - `self.handler` - for each reveived message
    - `self._monitoring` - monitor runned handlers and remove them if done
    """
    running = False

    task: asyncio.Task
    monitoring_task: asyncio.Task

    def __init__(self, queue, loop, debug=False):
        self.loop = loop
        self.queue = queue
        self.debug = debug

        self.msg_tasks = []

        self.configure_logger()

    def configure_logger(self):
        """Configure logger.
        """
        self.log = LoggerAdapter(
            logging.getLogger(__name__),
            {'name': self.__class__.__name__}
        )

    async def start(self):
        """Start consumer and monitoring tasks.
        """
        self.running = True
        self._consume()
        self.monitoring_task = self.loop.create_task(self._monitoring())

    def _consume(self):
        """Consume coroutine.

        Create task for incoming message.
        """
        self.queue.consume(self._handler)

    async def _monitoring(self):
        while self.running:
            for task in self.msg_tasks:
                if task.done():
                    self.msg_tasks.remove(task)
                    del task
            await asyncio.sleep(0.5)

    # pylint: disable=unused-argument
    def _handler(self, channel, basic_deliver, properties, body):
        self.log.info('Start task execution (_handler): %s', body)
        # pylint: disable=c-extension-no-member
        task = self.loop.create_task(
            self.handler(ujson.loads(body))
        )
        self.msg_tasks.append(task)
        channel.basic_ack(basic_deliver.delivery_tag)

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
        self.queue.close()

        await asyncio.sleep(0)

        if getattr(self, 'monitoring_task', False):
            await asyncio.wait_for(self.monitoring_task, 2)
            self.log.info("Monitor task cancelled")

        self.queue.close()

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
    pass


# pylint: disable=abstract-method
class EventTypedConsumer(BaseConsumer):
    """Event typed consumer.
    """
    def __init__(self, event_type, *, queue, loop):
        self.event_type = event_type
        super().__init__(queue=queue, loop=loop)


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


class BaseMessageConsumer(MessageConsumerMixIn, EventTypedConsumer):

    """Message consumer.

    Base class, transforms amqp message to internal Message object.
    """

    pass
