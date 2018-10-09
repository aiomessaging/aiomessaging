"""Messaging application.
"""
import os
import asyncio
import logging
import logging.config
from collections import defaultdict

from .config import Config
from .consumers import ConsumersManager
from .consumers import GenerationConsumer
from .cluster import Cluster
from .router import Router
from .queues import QueueBackend


# pylint: disable=too-many-instance-attributes
class AiomessagingApp:
    """aiomessaging application.
    """
    cluster: Cluster
    queue: QueueBackend
    config: Config

    generation_consumer: GenerationConsumer
    generation_listener = None

    consumers: ConsumersManager

    log: logging.Logger

    def __init__(self, config=None, loop=None):
        self.loop = loop
        self.consumers = ConsumersManager(self)

        self.set_event_loop(loop)

        self.config = Config()
        if config:
            self.config.from_file(config)

        self.configure_logging()
        self.log.info('Configuration file: %s', config)

        self.queue = self.config.get_queue_backend()

    # pylint: disable=no-self-use
    def event_types(self):
        """Get event types served by this instance.
        """
        return ['example_event']

    def start(self, loop=None):
        """Start aiomessaging application.
        """
        self.log.info("aiomessaging service was started. PID: %i",
                      os.getpid())
        assert self.config is not None, "Config not provided"

        self.loop = loop or self.loop or asyncio.get_event_loop()
        self.loop.set_debug(True)

        self.loop.run_until_complete(self._start())

        try:
            self.loop.run_forever()
        except KeyboardInterrupt:  # pragma: no cover
            print(" — Ctrl + C was pressed")
            self.log.info("Graceful shutdown. Press Ctrl + C to exit")
        finally:
            try:
                self.loop.run_until_complete(self.shutdown())
                self.loop.run_until_complete(self.loop.shutdown_asyncgens())
                self.loop.close()
                self.log.info("Loop closed.")
            except KeyboardInterrupt:  # pragma: no cover
                print(" — Ctrl + C was pressed second time")
                self.log.error("Stopped hard. Exiting.")
                exit(1)

    async def _start(self):
        """Start all application coroutines.
        """
        await self.queue.connect()

        await self.create_cluster()
        await self.consumers.start_all()

    async def create_cluster(self):
        """Create Cluster instance and start cluster queue handling.
        """
        queue = await self.queue.cluster_queue()
        self.cluster = Cluster(queue=queue, loop=self.loop)
        await self.cluster.start()

        self.generation_listener = self.loop.create_task(
            self.listen_generation()
        )

    async def listen_generation(self):
        """Listen generation queue of cluster for queue names to consume.

        TODO: rename

        Creates consumer for generated messages when cluster event received.
        """
        self.log.debug("Listen clusters generation queue")

        messages_queue = await self.queue.messages_queue(
            'example_event'
        )
        self.generation_consumer = GenerationConsumer(
            messages_queue=messages_queue, loop=self.loop
        )
        await self.generation_consumer.start()

        while True:
            queue_name = await self.cluster.generation_queue.get()
            self.log.debug('Message in generation_queue %s', queue_name)
            queue = await self.queue.generation_queue(name=queue_name)
            self.generation_consumer.consume(queue)

    async def stop_listen_generation(self):
        """Stop listen for generation queues.
        """
        await self.generation_consumer.stop()
        self.generation_listener.cancel()

    async def send(self, event_type, payload=None):
        """Publish event to the events queue.
        """
        if not self.queue.is_open:
            await self.queue.connect()

        queue = await self.queue.events_queue(event_type)
        # because we publish to '' exchange by default
        routing_key = "events.%s" % event_type
        await queue.publish(payload, routing_key=routing_key)

    def get_router(self, event_type) -> Router:
        """Get router instance for event type.
        """
        router_config = self.config.events.get(event_type)['output']
        return Router(router_config)

    def configure_logging(self):
        """Configure logging.
        """
        self.log = logging.getLogger(__name__)
        logging.basicConfig(
            format=self.config.get_log_format()
        )

        if self.config.is_testing:
            # skip config for tests, because it replaces caplog handlers
            return

        logging.config.dictConfig(self.config.get_logging_dict())  # pragma: no cover

    def set_event_loop(self, loop):
        """Set event loop to run on.
        """
        self.loop = loop or asyncio.get_event_loop()
        self.loop.set_debug(True)

    def stop(self):
        """Stop application event loop.
        """
        self.log.debug('Stopping event loop')
        self.loop.stop()

    async def shutdown(self):
        """Shutdown application gracefully.
        """
        await self.stop_listen_generation()
        await self.cluster.stop()

        await self.consumers.stop_all()

        await self.queue.close()
        self.log.info("Shutdown complete.")
