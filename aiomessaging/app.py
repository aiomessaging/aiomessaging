"""Messaging application.
"""
import os
import asyncio
import logging

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

    generation_listener = None
    generation_monitor: asyncio.Task

    log: logging.Logger

    def __init__(self, config=None, loop=None):
        self.event_consumers = {}
        self.message_consumers = {}
        self.output_consumers = {}
        self.started_generators = []
        self.loop = loop

        self.set_event_loop(loop)

        self.config = Config()
        if config:
            self.config.from_file(config)

        self.configure_logging()
        self.log.debug('Configuration file: %s', config)

        self.queue = self.config.get_queue_backend()

    # pylint: disable=no-self-use
    def event_types(self):
        """Get event types served by this instance.
        """
        return ['example_event']

    def start(self, loop=None):
        """Start aiomessaging application.
        """
        assert self.config is not None, "Config not provided"

        self.loop = loop or self.loop or asyncio.get_event_loop()
        self.loop.set_debug(True)

        self.loop.run_until_complete(self._start())

        try:
            self.log.info("aiomessaging service was started. PID: %i",
                          os.getpid())
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
        exchange = await self.queue.cluster_queue()
        self.cluster = Cluster(queue=queue, exchange=exchange, loop=self.loop)
        await self.cluster.start()
        self.generation_listener = self.loop.create_task(
            self.listen_generation()
        )
        self.generation_monitor = self.loop.create_task(
            self.monitor_generation()
        )

    async def listen_generation(self):
        """Listen generation queue of cluster for queue names to consume.

        FIXME: very bad unmeaning name

        Creates consumer for generated messages when cluster event recieved.
        """
        self.log.info("Listen clusters generation queue")
        while True:
            queue_name = await self.cluster.generation_queue.get()
            self.log.info('Message in generation_queue %s', queue_name)
            queue = await self.queue.generation_queue(name=queue_name)
            # TODO: we need a correct type
            messages_queue = await self.queue.messages_queue(
                'example_event'
            )
            gen = GenerationConsumer(
                queue=queue, messages_queue=messages_queue,
                loop=self.loop
            )
            await gen.start()
            self.started_generators.append(gen)

    async def monitor_generation(self):
        """Generation monitoring coroutine.
        """
        self.log.debug("Start generators monitoring")
        while True:
            for gen in self.started_generators:
                if not gen.running:
                    self.log.debug('Free ended generator')
                    self.started_generators.remove(gen)
                    del gen
            await asyncio.sleep(1)

    async def stop_listen_generation(self):
        """Stop listen for generation queues.
        """
        self.generation_listener.cancel()
        self.generation_monitor.cancel()
        # handle errors from listen_generation
        try:
            exc = None
            exc = self.generation_listener.exception()
            await asyncio.wait_for(self.generation_listener, 5)
        except asyncio.InvalidStateError:
            pass
        if exc:  # pragma: no cover
            self.log.error("Generation listner exception found %s.", exc)
            raise exc

        for item in self.started_generators:
            await item.stop()
        self.log.info('Stop %s gens', len(self.started_generators))

    async def create_event_consumers(self):
        """Create consumers for each event type.
        """
        for event_type in self.event_types():
            event_pipeline = self.config.get_event_pipeline(event_type)
            generators = self.config.get_generators(event_type)

            self.event_consumers[event_type] = EventConsumer(
                event_type,
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
        router = Router()

        for event_type in self.event_types():
            self.log.info("Create event consumer for type %s", event_type)
            self.message_consumers[event_type] = MessageConsumer(
                event_type,
                router=router,
                output_queue=await self.queue.output_queue(event_type),
                queue=await self.queue.messages_queue(event_type),
                loop=self.loop
            )
            await self.message_consumers[event_type].start()

    async def create_output_consumers(self):
        """Create output consumers.
        """
        for event_type in self.event_types():
            queue = await self.queue.output_queue(event_type)
            self.output_consumers[event_type] = OutputConsumer(
                event_type,
                queue=queue,
                loop=self.loop
            )
            await self.output_consumers[event_type].start()

    def configure_logging(self):
        """Configure logging.
        """
        self.log = logging.getLogger('aiomessaging')
        # if self.config.app.debug:
        #     loglevel = logging.INFO
        # else:
        #     loglevel = logging.INFO
        logging.basicConfig(
            format=self.config.get_log_format()
        )
        self.log.setLevel(logging.DEBUG)

    def set_event_loop(self, loop):
        """Set event loop to run on.
        """
        self.loop = loop

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
        self.log.debug("Cluster stopped")

        await stop_all(self.event_consumers)
        await stop_all(self.message_consumers)
        await stop_all(self.output_consumers)

        await self.queue.close()
        self.log.info("Shutdown complete.")


async def stop_all(consumers):
    """Stop all consumers helper.
    """
    # pylint: disable=expression-not-assigned
    [await a.stop() for a in consumers.values()]


if __name__ == '__main__':
    # TODO: remove before release
    # pylint: disable=invalid-name
    app = AiomessagingApp('example.yml')  # pragma: no cover
    app.start()  # pragma: no cover
