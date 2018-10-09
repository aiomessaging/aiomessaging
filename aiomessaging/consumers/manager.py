"""
Consumers manager.
"""
import asyncio
from typing import Dict
from collections import defaultdict

from ..router import Router

from .event import EventConsumer
from .message import MessageConsumer
from .output import OutputConsumer
from .generation import GenerationConsumer


class ConsumersManager:

    """Consumers manager.

    Container for all application consumers except cluster.
    """

    event_consumers: Dict[str, EventConsumer]
    message_consumers: Dict[str, MessageConsumer]
    output_consumers: Dict[str, Dict]

    generation_consumer: GenerationConsumer
    generation_listener: asyncio.Task

    def __init__(self, app):
        self.app = app

        # TODO: move to constructor arguments
        self.config = app.config
        self.queue = app.queue
        self.generation_queue = app.generation_queue
        self.loop = app.loop

        # TODO: replace with different logger
        self.log = app.log

        self.event_consumers = {}
        self.message_consumers = {}
        self.output_consumers = defaultdict(dict)

    async def start_all(self):
        """Start all common consumers.
        """
        await self.create_event_consumers()
        await self.create_message_consumers()
        await self.create_output_consumers()

        self.generation_listener = self.loop.create_task(
            self.listen_generation()
        )

    async def stop_all(self):
        """Stop all started consumers.
        """
        await self.stop_listen_generation()

        await stop_all(self.event_consumers)
        await stop_all(self.message_consumers)
        for group in self.output_consumers.values():
            await stop_all(group)

    async def create_event_consumers(self):
        """Create event consumers for each event type.
        """
        for event_type in self.event_types():
            event_pipeline = self.config.get_event_pipeline(event_type)
            generators = self.config.get_generators(event_type)

            self.event_consumers[event_type] = EventConsumer(
                event_type=event_type,
                event_pipeline=event_pipeline,
                generators=generators,
                generation_queue=self.generation_queue,
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
            self.log.debug("Create MessageConsumer for type %s", event_type)

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
                messages_queue = await self.queue.messages_queue(
                    event_type
                )
                self.output_consumers[output][event_type] = OutputConsumer(
                    router=self.get_router(event_type),
                    event_type=event_type,
                    messages_queue=messages_queue,
                    queue=queue,
                    loop=self.loop
                )
                await self.output_consumers[output][event_type].start()

    async def listen_generation(self):
        """Listen generation queue of cluster for queue names to consume.

        TODO: rename

        Creates consumer for generated messages when cluster event received.
        """
        self.log.debug("Listen clusters generation queue")

        self.generation_consumer = GenerationConsumer(
            messages_queue=await self.queue.messages_queue('example_event'),
            loop=self.loop
        )
        await self.generation_consumer.start()

        while True:
            queue_name = await self.generation_queue.get()
            self.log.debug('Message in generation_queue %s', queue_name)
            queue = await self.queue.generation_queue(name=queue_name)
            self.generation_consumer.consume(queue)

    async def stop_listen_generation(self):
        """Stop listen for generation queues.
        """
        await self.generation_consumer.stop()
        self.generation_listener.cancel()

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
