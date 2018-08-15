"""Messaging queue backend.
"""
import logging
import asyncio

from typing import Dict

import pika
import ujson

from ..utils import gen_id

from .queue import Queue


logger = logging.getLogger(__name__)

# declare operations timeout
DECLARE_CHANNEL_TIMEOUT = 1
DECLARE_EXCHANGE_TIMEOUT = 1
DECLARE_QUEUE_TIMEOUT = 1


# pylint: disable=too-many-instance-attributes
class QueueBackend:
    """Queue backend implementation.
    """

    TYPE_FANOUT = 'fanout'
    TYPE_DIRECT = 'direct'

    reconnect_timeout: float

    # set to True before expected close
    _normal_close = False
    _reconnect_task = None

    _channels: Dict[str, pika.channel.Channel]
    _channels_opening: Dict[str, asyncio.Future]

    def __init__(self, host='localhost', port=5672, username='guest',
                 password='guest', virtual_host="/", loop=None,
                 reconnect_timeout=3):
        self.loop = loop
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.virtual_host = virtual_host
        self.reconnect_timeout = reconnect_timeout

        self.log = logger

        self.connection = None

        self._connecting = False
        self._closing = False

        self._channels_opening = {}
        self._channels = {}

    # pylint: disable=no-self-use
    def get_url(self):
        """Connection string.
        """
        # TODO: use parameters from instance
        return ('amqp://guest:guest@localhost:5672/?connection_attempts=10'
                '&retry_delay=2')

    def connect(self, loop=None):
        """Establish connection to queue backend.
        """
        if loop:
            self.loop = loop
        if not self.loop:
            self.loop = loop = asyncio.get_event_loop()

        self._connecting = self._create_future()
        self._closing = self._create_future()

        self.connection = pika.adapters.AsyncioConnection(
            pika.URLParameters(self.get_url()),
            self.on_connection_open,
            self.on_open_error_callback,
            self.on_connection_closed,
            custom_ioloop=self.loop,
        )
        return self._connecting

    @property
    def is_open(self):
        """Connection opened flag.
        """
        return self.connection.is_open

    async def channel(self, name='default'):
        """Get new channel for connection (coroutine).
        """
        future = asyncio.Future(loop=self.loop)

        if not self._connecting.done():  # pragma: no cover
            self.log.debug('Await connecting...')
            await self._connecting

        if name in self._channels_opening:
            if not self._channels_opening[name].done():
                self.log.debug('Channel already opening, wait it...')
                return await self._channels_opening[name]

        if name in self._channels and self._channels[name].is_open:
            future.set_result(self._channels[name])
            return await future

        self._channels_opening[name] = self._create_future()

        def on_channel(channel: pika.channel.Channel):
            """On channel closed handler.
            """
            channel.add_on_close_callback(self.on_channel_closed)
            channel.basic_qos(prefetch_count=20)
            self._channels[name] = channel
            try:
                self._channels_opening[name].set_result(channel)
            except asyncio.InvalidStateError:  # pragma: no cover
                pass
            future.set_result(channel)

        self.connection.channel(on_open_callback=on_channel)
        return await asyncio.wait_for(future, timeout=DECLARE_CHANNEL_TIMEOUT)

    async def publish(self, exchange, routing_key, body):
        """Publish message to queue.

        DEPRECATED
        """
        channel = await self.channel()
        properties = pika.BasicProperties(app_id='example-publisher',
                                          content_type='application/json')
        # pylint: disable=c-extension-no-member
        channel.basic_publish(exchange, routing_key,
                              ujson.dumps(body, ensure_ascii=False),
                              properties)
        channel.close()

    def on_channel_closed(self, channel, reply_code, reply_text):
        """Handle channel closed event.
        """
        self.log.debug('CHANNEL%i closed', channel.channel_number)

    def on_open_error_callback(self, *args, **kwargs):  # pragma: no cover
        """Opening error callback.
        """
        # TODO: args
        self.log.error('Opening error. Args: %s Kwargs: %s', args, kwargs)

    def on_connection_open(self, connection):
        """Connection opened callback.
        """
        self._reconnect_task = None
        self._connecting.set_result(True)

    def on_connection_closed(self, connection, reply_code, reply_text):
        """Connection closed callback.
        """
        if self._normal_close:
            self.log.info('Connection closed')
        else:
            self.log.error('Connection closed unexpectedly: %s %s',
                           reply_code, reply_text)

        # cancel _connecting Future
        if self._connecting and not self._connecting.done():  # pragma: no cover
            self.log.error('Cancel _connecting it is not done')
            self._connecting.cancel()

        # resolve _closing Future
        self._closing.set_result((reply_code, reply_text))

        if not self._normal_close:
            self.log.warning("Not a normal shutdown. Reconnecting after 3s.")
            self.loop.call_later(self.reconnect_timeout, self.reconnect)

    def reconnect(self):
        """Reconnect.
        """
        if self._reconnect_task:  # pragma: no cover
            self.log.debug('Another reconnection task active')
            return
        self.log.debug('Create new connect task')
        self._reconnect_task = self.loop.create_task(
            asyncio.wait([self.connect()])
        )

    def close(self) -> asyncio.Future:
        """Close connection.
        """
        self._normal_close = True
        self.connection.close()
        return self._closing  # future will be resolved after connection close

    async def get_queue(self, *args, **kwargs) -> Queue:
        """Get queue for backend.
        """
        queue = Queue(self, *args, **kwargs)
        self.log.debug("Start declare queue...")
        return await queue.declare()

    async def events_queue(self, event_type) -> Queue:
        """Get events queue.
        """
        name = f"events.{event_type}"
        return await self.get_queue(name, auto_delete=False, durable=True)

    async def generation_queue(self, event_type=None, name=None) -> Queue:
        """Declare tmp generation queue.
        """
        if not any([event_type, name]):  # pragma: no cover
            raise Exception("You must provide event_type or name")
        if name is None:
            name = gen_id(f"gen.{event_type}")
        self.log.info('Generation queue %s, %s', name, event_type)
        return await self.get_queue(
            name=name, exchange='', exchange_type=self.TYPE_DIRECT,
            routing_key=name, auto_delete=True
        )

    async def messages_queue(self, event_type) -> Queue:
        """Get messages queue.
        """
        return await self.get_queue(
            name=f"messages.{event_type}",
            auto_delete=False, durable=True,
            exchange=f'messages.{event_type}', exchange_type=self.TYPE_DIRECT,
            routing_key=event_type
        )

    async def cluster_queue(self) -> Queue:
        """Get cluster queue.
        """
        return await self.get_queue(
            name=gen_id('cluster.node'),
            auto_delete=True,
            durable=False,

            exchange='cluster',
            exchange_type=self.TYPE_FANOUT,
            routing_key=''
        )

    async def output_queue(self, event_type, output_name=None) -> Queue:
        """Get output queue.
        """
        name = f"output.{event_type}"
        return await self.get_queue(
            name=name,
            auto_delete=False,
            durable=True,

            exchange=name,
            exchange_type=self.TYPE_DIRECT,
            routing_key=output_name
        )

    def _create_future(self):
        """Create future bounded to backend loop.
        """
        return asyncio.Future(loop=self.loop)
