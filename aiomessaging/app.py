"""Messaging application.
"""
import os
import asyncio
import logging
import logging.config
from collections import defaultdict

from .config import Config
from .consumers import EventConsumer
from .consumers import GenerationConsumer
from .consumers import MessageConsumer
from .consumers import OutputConsumer
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

    log: logging.Logger

    def __init__(self, config=None, loop=None):
        self.event_consumers = {}
        self.message_consumers = {}
        self.output_consumers = defaultdict(dict)
        self.loop = loop

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
        await self.create_event_consumers()
        await self.create_message_consumers()
        await self.create_output_consumers()

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

    async def create_event_consumers(self):
        """Create consumers for each event type.
        """
        for event_type in self.event_types():
            event_pipeline = self.config.get_event_pipeline(event_type)
            generators = self.config.get_generators(event_type)

            self.event_consumers[event_type] = EventConsumer(
                event_type=event_type,
                event_pipeline=event_pipeline,
                generators=generators,
                cluster=self.cluster,
                queue=await self.queue.events_queue(event_type),
                # TODO: replace with tmp queue factory?
                queue_service=self.queue,
                loop=self.loop,
            )
            await self.event_consumers[event_type].start()

    async def create_message_consumers(self):
        """Create message consumers.
        """

        for event_type in self.event_types():
            self.log.debug("Create event consumer for type %s", event_type)

            self.message_consumers[event_type] = MessageConsumer(
                event_type,
                router=self.get_router(event_type),
                output_queue=await self.queue.output_queue(event_type),
                available_outputs=self.config.get_enabled_outputs(event_type),
                queue=await self.queue.messages_queue(event_type),
                loop=self.loop
            )
            await self.message_consumers[event_type].start()

    async def create_output_consumers(self):
        """Create output consumers.
        """
        for event_type in self.event_types():
            for output in self.config.get_enabled_outputs(event_type):
                queue = await self.queue.output_queue(event_type, output)
                messages_queue = await self.queue.messages_queue(event_type)
                self.output_consumers[output][event_type] = OutputConsumer(
                    router=self.get_router(event_type),
                    event_type=event_type,
                    messages_queue=messages_queue,
                    queue=queue,
                    loop=self.loop
                )
                await self.output_consumers[output][event_type].start()

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

        await stop_all(self.event_consumers)
        await stop_all(self.message_consumers)
        for group in self.output_consumers.values():
            await stop_all(group)

        await self.queue.close()
        self.log.info("Shutdown complete.")


async def stop_all(consumers):
    """Stop all consumers.
    """
    # pylint: disable=expression-not-assigned
    [await a.stop() for a in consumers.values()]
