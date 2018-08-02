"""Queue.
"""
import logging
import asyncio

from functools import partial
from abc import ABC, abstractmethod, abstractproperty

import pika
import ujson

from ..logging import QueueLoggerAdapter


logger = logging.getLogger(__name__)


class AbstractQueue(ABC):

    """Abstract Queue.

    This interface must be implemented for each queue backend and must hide
    underlying backend-specific implementation.

    Only one consumer allowed per queue by design.
    """

    log: QueueLoggerAdapter

    def __init__(self):
        self.log = QueueLoggerAdapter(logger, self)

    @abstractproperty
    def name(self):
        """Queue name.

        Must be implemented.
        """
        raise NotImplementedError  # pragma: no cover

    @abstractmethod
    def consume(self, handler) -> None:
        """Start consume messages.

        Passed handler will be invoked when new message received.
        """
        pass  # pragma: no cover

    @abstractmethod
    def cancel(self) -> asyncio.Future:
        """Cancel consume queue.

        Gracefully stop consumption and close connection without any message
        loss. Queue responsible for unacked messages return and their
        persistence on the backend. Hides details from consumer.

        Return `Future` that will be resolved after successful cancellation.
        """
        pass  # pragma: no cover

    async def publish(self, body, routing_key=None):
        """Publish message to the queue.

        TODO: bad interface
        """
        pass  # pragma: no cover


# pylint: disable=too-many-instance-attributes
class Queue(AbstractQueue):

    """Queue.

    Abstraction above amqp queue and exchange. Adds asyncio sugar.

    You must provide `exchange`, `exchange_type` and `routing key` kwargs if
    you want to publish messages.

    Pass empty string to `name` if you want a random name.

    Queue handle reconnects by itself obtaining new channel from backend if
    current one was closed. (TODO)
    """

    _consume_handler = None
    _consumer_tag = None
    _channel: pika.channel.Channel
    _normal_close = False

    # pylint: disable=too-many-arguments
    def __init__(self, backend, name=None, exchange=None, exchange_type=None,
                 routing_key=None, auto_delete=True, durable=False):
        self._name = name
        super().__init__()

        self._backend = backend

        self.exchange = exchange
        self.exchange_type = exchange_type
        self.routing_key = routing_key

        self.auto_delete = auto_delete
        self.durable = durable

        assert self.exchange or self.name is not None, \
            ("You must define name if you want to consume queue"
             "or exchange if you want to publish to this queue.")

    @property
    def name(self):
        return self._name

    async def declare(self) -> 'Queue':
        """Declare required queue and exchange.

        Queue, exchange and binding will be declared if information provided.
        """
        # we are relying to this in other functions
        self._channel = await self._backend.channel()
        self.log.debug("Channel acquired CHANNEL%i",
                       self._channel.channel_number)

        if self.exchange:
            await self.declare_exchange()

        if self.name is not None:
            await self.declare_queue()

        if self.exchange:
            await self.bind_queue()

        return self

    def consume(self, handler):
        """Start consume queue.

        You must pass handler to start consume.
        """
        bounded_handler = partial(handler, self)
        self._consume_handler = handler
        self.log.debug("Start consuming")
        self._channel.add_on_close_callback(
            self.on_channel_closed
        )
        self._consumer_tag = self._channel.basic_consume(bounded_handler,
                                                         self.name)
        self.log.debug("Consumer tag %s on CHANNEL%i",
                       self._consumer_tag, self._channel.channel_number)

    async def declare_and_consume(self, handler):
        """Declare queue and consume.

        Used in reconnect.
        """
        try:
            await self.declare()
            self.consume(handler)
        except pika.exceptions.ChannelClosed:  # pragma: no cover
            self.reconnect()

    async def publish(self, body, routing_key=None):
        """Publish message to the queue using exchange.
        """
        properties = pika.BasicProperties(
            app_id='example-publisher',
            content_type='application/json'
        )
        self.log.debug("Publish to %s:%s", self.exchange,
                       routing_key or self.routing_key)
        channel = await self._backend.channel('publish')
        try:
            channel.basic_publish(
                self.exchange,
                routing_key or self.routing_key or '',
                # pylint: disable=c-extension-no-member
                ujson.dumps(body, ensure_ascii=False),
                properties)
        except pika.exceptions.ChannelClosed:
            self.log.error(
                'Message not delivered (%s): %s',
                routing_key, body
            )

    async def delete(self):
        """Delete queue explicitly.
        """
        # pylint: disable=protected-access
        future = self._backend._create_future()

        # pylint: disable=unused-argument
        def on_delete(method_frame):
            future.set_result(True)
            self.log.debug('deleted')

        self._channel.queue_delete(callback=on_delete, queue=self.name)
        self.log.debug('delete')

        return future

    async def declare_exchange(self) -> asyncio.Future:
        """Declare exchange for queue.
        """
        # pylint: disable=protected-access
        future = self._backend._create_future()

        def on_declare_exchange(frame):
            future.set_result(frame)
            self.log.debug('Exchange `%s` declared ok', self.exchange)

        self._channel.exchange_declare(
            on_declare_exchange,
            self.exchange,
            self.exchange_type
        )

        return future

    def declare_queue(self) -> asyncio.Future:
        """Declare amqp queue.
        """
        # pylint: disable=protected-access
        future = self._backend._create_future()

        def on_queue_declare(method_frame):
            # TODO: there is a race condition (check)
            self._name = method_frame.method.queue
            future.set_result(method_frame)
            self.log.debug('Declared ok')

        self._channel.queue_declare(
            on_queue_declare, self.name, auto_delete=self.auto_delete,
            durable=self.durable
        )

        self.log.debug('Declaring queue itself')

        return future

    def bind_queue(self):
        """Bind queue to exchange.
        """
        # pylint: disable=protected-access
        future = self._backend._create_future()

        def on_bindok(unused_frame):
            future.set_result(True)

        self.log.debug('Bind queue exchange=%s, routing_key=%s',
                       self.exchange, self.routing_key)
        self._channel.queue_bind(on_bindok, self.name,
                                 self.exchange, self.routing_key)

        return future

    def on_channel_closed(self, *args, **kwargs):
        """Handle channel closed event.

        Call reconnect after timeout.
        """
        if not self._normal_close:
            self.log.warning(
                'Channel closed. Reconnect after 5s. args: %s, kwargs: %s',
                args, kwargs
            )
            self._backend.loop.call_later(self._backend.reconnect_timeout,
                                          self.reconnect)

    def on_consume_cancelled(self, *args, **kwargs):
        """Handle consume cancelled event.
        """
        self.log.warning(
            'Consume cancelled. Reconnect after 5s. args: %s, kwargs: %s',
            args, kwargs
        )
        self._backend.loop.call_later(self._backend.reconnect_timeout,
                                      self.reconnect)

    def reconnect(self):
        """
        TODO: handle disconnects?
        TODO: refactoring
        """
        self.log.info("Reconnecting %s", self.name)
        if self._backend.is_open:
            try:
                # restart with previously saved handler
                if self._consume_handler:
                    self._backend.loop.create_task(
                        self.declare_and_consume(self._consume_handler)
                    )
                else:
                    self.log.error(
                        'No consume handler found while reconnecting')
            except pika.exceptions.ChannelClosed:
                self.log.warning('Channel closed, reconnect')
                self._backend.loop.call_later(self._backend.reconnect_timeout,
                                              self.reconnect)
        else:
            self.log.warning('Connection still lost. Retry after 5s.')
            self._backend.loop.call_later(self._backend.reconnect_timeout,
                                          self.reconnect)

    def need_declare_queue(self):
        """Check if we need to declare queue.

        Return True if queue name defined so we can consume messages from our
        abstract queue.
        """
        return self.name is not None

    def close(self):
        """Close queue and channel.
        """
        self._normal_close = True

        self.cancel()

    def cancel(self):
        """Stop consume messages from queue.
        """
        def on_cancelok(method_frame):
            """Handle cancelok.
            """
            self.log.debug("Cancel ok on CHANNEL%s",
                           method_frame.channel_number)
        try:
            if self._consumer_tag:
                self._channel.basic_cancel(
                    on_cancelok,
                    self._consumer_tag
                )
        except pika.exceptions.ChannelClosed:
            self.log.warning('Channel already closed while closing queue')

    def on_cancelok(self, method_frame):
        """Handle cancelok.
        """
        self.log.debug("Cancel ok on CHANNEL%s", method_frame.channel_number)

    def __repr__(self):
        """Queue representation.
        """
        return (f'<Queue (name={self.name};exchange={self.exchange};'
                f'routing_key={self.routing_key}>')
