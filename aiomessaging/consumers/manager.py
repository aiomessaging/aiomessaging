"""
Consumers manager.
"""
import logging
import asyncio
from typing import Dict
from collections import defaultdict

from ..queues import QueueBackend
from ..config import Config
from ..router import Router
from ..cluster import Cluster

from .event import EventConsumer
from .message import MessageConsumer
from .output import OutputConsumer
from .generation import GenerationConsumer


# pylint: disable=too-many-instance-attributes
class ConsumersManager:

    """Consumers manager.

    Container for all application consumers.
    """

    config: Config
    queue: QueueBackend

    loop: asyncio.AbstractEventLoop
    cluster: Cluster

    event_consumers: Dict[str, EventConsumer]
    message_consumers: Dict[str, MessageConsumer]
    output_consumers: Dict[str, Dict[str, OutputConsumer]]

    generation_consumer: GenerationConsumer

    def __init__(self, config, queue: QueueBackend, loop=None):
        self.config = config
        self.queue = queue
        self.loop = loop

        self.log = logging.getLogger(__name__)

        self.event_consumers = {}
        self.message_consumers = {}
        self.output_consumers = defaultdict(dict)

    async def start_all(self, loop=None):
        """Start all common consumers.
        """
        if loop:
            self.loop = loop

        await self.start_generation_consumer()
        await self.create_cluster()
        await self.create_event_consumers()
        await self.create_message_consumers()

    async def stop_all(self):
        """Stop all started consumers.
        """
        await self.cluster.stop()

        await self.stop_generation_consumer()

        await stop_all(self.event_consumers)
        await stop_all(self.message_consumers)

        for group in self.output_consumers.values():
            await stop_all(group)

    async def create_cluster(self):
        """Create Cluster instance and start cluster queue handling.
        """
        queue = await self.queue.cluster_queue()
        self.cluster = Cluster(
            queue=queue,
            loop=self.loop
        )
        self.cluster.on_start_consume(self.consume_generation_queue)
        self.cluster.on_output_observed(self.on_cluster_output_observed)
        await self.cluster.start()

    async def on_cluster_output_observed(self, event_type, output):
        """Handle output observation from cluster.
        """
        await self.start_output_consumer(event_type, output.name)

    async def consume_generation_queue(self, queue_name):
        """Consume tmp queue.

        Invoked by Cluster instance when `start_consume` action received.
        """
        self.log.debug('Start consume tmp queue %s (start_consume received '
                       'from cluster)', queue_name)
        queue = await self.queue.generation_queue(name=queue_name)
        self.generation_consumer.consume(queue)

    async def create_event_consumers(self):
        """Create event consumers for each event type.
        """
        for event_type in self.event_types():
            event_pipeline = self.config.get_event_pipeline(event_type)
            generators = self.config.get_generators(event_type)

            consumer = EventConsumer(
                event_type=event_type,
                event_pipeline=event_pipeline,
                generators=generators,
                queue=await self.queue.events_queue(event_type),
                # TODO: replace with tmp queue factory?
                queue_service=self.queue,
                loop=self.loop,
            )
            consumer.on_generation_complete(self.cluster.start_consume)
            await consumer.start()
            self.event_consumers[event_type] = consumer

    async def create_message_consumers(self):
        """Create message consumers.
        """

        for event_type in self.event_types():
            self.log.debug("Create MessageConsumer for type %s", event_type)

            consumer = MessageConsumer(
                event_type,
                router=self.get_router(event_type),
                output_queue=await self.queue.output_queue(event_type),
                queue=await self.queue.messages_queue(event_type),
                loop=self.loop
            )
            consumer.on_output_observed(self.on_output_observed)
            await consumer.start()
            self.message_consumers[event_type] = consumer

    async def on_output_observed(self, event_type: str, output):
        """Output observation handler.

        Start output consumer and notify cluster on output observed.
        """
        self.log.debug("New output observed: %s", output.name)
        # await self.start_output_consumer(event_type, output.name)
        await self.cluster.output_observed(event_type, output)

    async def start_output_consumer(self, event_type, output):
        """Start output consumer for provided event type.

        :param str event_type: event type
        :param str output: output backend name
        """
        if output in self.output_consumers and event_type in self.output_consumers[output]:
            return
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

    async def start_generation_consumer(self):
        """Listen generation queue of cluster for queue names to consume.

        Creates consumer for generated messages when cluster event received.
        """
        self.log.debug("Listen clusters generation queue")

        self.generation_consumer = GenerationConsumer(
            messages_queue=await self.queue.messages_queue('example_event'),
            loop=self.loop
        )
        await self.generation_consumer.start()

    async def stop_generation_consumer(self):
        """Stop listen for generation queues.
        """
        await self.generation_consumer.stop()

    # pylint: disable=no-self-use
    def event_types(self):
        """Get event types served by this instance.

        FIXME
        """
        return ['example_event']

    def get_router(self, event_type) -> Router:
        """Get router instance for event type.
        """
        router_config = self.config.events.get(event_type)['output']
        return Router(router_config)


async def stop_all(consumers):
    """Stop all consumers.
    """
    # pylint: disable=expression-not-assigned
    [await a.stop() for a in consumers.values()]
