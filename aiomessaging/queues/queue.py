import logging
import asyncio

import pika
import ujson


logger = logging.getLogger(__name__)


class Queue:

    """Queue.

    Abstraction above amqp queue and exchange. Adds asyncio sugar.

    You must provide `exchange`, `exchange_type` and `routing key` kwargs if
    you want to publish messages.

    Pass empty string to `name` if you want a random name.

    Queue handles reconnects by itself obtaining new channel from backend if
    current one was closed. (TODO)
    """

    _consume_handler = None
    _consumer_tag = None
    _channel: pika.channel.Channel
    _normal_close = False

    def __init__(self, backend, name=None, exchange=None, exchange_type=None,
                 routing_key=None, auto_delete=True, durable=False):
        self._backend = backend
        self.log = logger

        self.exchange = exchange
        self.exchange_type = exchange_type
        self.routing_key = routing_key

        self.name = name

        self.auto_delete = auto_delete
        self.durable = durable

        self.channel = None

        assert self.need_declare_exchange() or self.need_declare_queue(), \
            ("You must define name if you want to consume queue"
             "or exchange, exchange_type and routing_key if you want to "
             "publish (to exchange).")

    async def declare(self) -> 'Queue':
        """Declare required queue and exchange.

        Queue, exchange and binding will be declared if information provided.
        See `need_declare_queue` and `need_declare_exchange` methods for
        details.
        """
        self.log.info("Acquire channel for queue %s:%s",
                      self.exchange, self.name)
        self._channel = await self._backend.channel()

        if self.need_declare_exchange():
            await self.declare_exchange()

        if self.need_declare_queue():
            await self.declare_queue()

        if self.need_declare_queue() and self.need_declare_exchange():
            await self.bind_queue()

        return self

    async def declare_exchange(self) -> asyncio.Future:
        future = self._backend._create_future()

        def on_declare_exchange(frame):
            future.set_result(frame)
            self.log.debug('Exchange declared ok: %s', self.exchange)

        self._channel.exchange_declare(
            on_declare_exchange,
            self.exchange,
            self.exchange_type
        )

        return future

    async def declare_queue(self) -> asyncio.Future:
        future = self._backend._create_future()

        def on_queue_declare(method_frame):
            # FIXME: there is a race condition
            self.name = method_frame.method.queue
            future.set_result(method_frame)
            self.log.debug('Declared ok: %s', method_frame)

        self._channel.queue_declare(
            on_queue_declare, self.name, auto_delete=self.auto_delete,
            durable=self.durable
        )

        self.log.debug('Declaring queue: %s, %s:%s',
                       self.name, self.exchange, self.routing_key)

        return future

    async def bind_queue(self):
        future = self._backend._create_future()

        def on_bindok(unused_frame):
            future.set_result(True)
        self.log.debug('Bind queue %s', self.name)
        self._channel.queue_bind(on_bindok, self.name,
                                 self.exchange, self.routing_key)

        return future

    def consume(self, handler):
        """Start consume queue.

        You must pass handler to start consume.
        """
        # FIXME: only one consumer per queue allowed in case of reconnect
        self._consume_handler = handler
        self.log.info("%s: Start consume", self.name)
        self._channel.add_on_close_callback(self.on_channel_closed)
        self._consumer_tag = self._channel.basic_consume(handler,
                                                         self.name)
        self.log.info("Consumer tag (%s) %s", id(self), self._consumer_tag)

    async def declare_and_consume(self, handler):
        try:
            await self.declare()
            self.consume(handler)
        except pika.exceptions.ChannelClosed:
            self.reconnect()

    async def publish(self, body, routing_key=None):
        properties = pika.BasicProperties(
            app_id='example-publisher',
            content_type='application/json'
        )
        self.log.info("Publish to %s:%s", self.exchange,
                      routing_key or self.routing_key)
        channel = await self._backend.publish_channel(reuse=False)
        try:
            channel.basic_publish(
                self.exchange,
                routing_key or self.routing_key or '',
                ujson.dumps(body, ensure_ascii=False),
                properties
            )
        except pika.exceptions.ChannelClosed:
            self.log.error(
                'Message not delivered (%s): %s',
                routing_key, body
            )
        channel.close()

    def on_channel_closed(self, *args, **kwargs):
        if not self._normal_close:
            self.log.warning(
                'Channel closed. Reconnect after 5s. args: %s, kwargs: %s',
                args, kwargs
            )
            self._backend.loop.call_later(5, self.reconnect)

    def on_consume_cancelled(self, *args, **kwargs):
        self.log.warning(
            'Consume cancelled. Reconnect after 5s. args: %s, kwargs: %s',
            args, kwargs
        )
        self._backend.loop.call_later(5, self.reconnect)

    def reconnect(self):
        """
        TODO: handle disconnects?
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
                self._backend.loop.call_later(5, self.reconnect)
        else:
            self.log.warning('Connection still lost. Retry after 5s.')
            self._backend.loop.call_later(5, self.reconnect)

    def need_declare_exchange(self):
        """Check if we need to declare exchange.

        Returns True if all of the exchange, exchange_type and routing_key
        defined so we can publish to our abstract queue.
        """
        return all([
            self.exchange,
            # self.exchange_type,
            # self.routing_key
        ])

    def need_declare_queue(self):
        """Check if we need to declare queue.

        Return True if queue name defined so we can consume messages from our
        abstract queue.
        """
        return self.name is not None

    def close(self):
        self._normal_close = True

        try:
            if self._consumer_tag:
                self._channel.basic_cancel(
                    self.on_cancelok, self._consumer_tag)
            if self._channel and self._channel.is_open:
                self._channel.close()
        except pika.exceptions.ChannelClosed:
            pass

    def on_cancelok(self, *args, **kwargs):
        self.log.info("Cancelok %s %s", args, kwargs)

    def __repr__(self):
        return f'<Queue (name={self.name};exchange={self.exchange};routing_key={self.routing_key}>'
