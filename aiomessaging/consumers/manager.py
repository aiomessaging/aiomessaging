"""
Consumers manager.
"""
import asyncio
from typing import Dict
from collections import defaultdict

from .event import EventConsumer
from .message import MessageConsumer
from .output import OutputConsumer
from .generation import GenerationConsumer


class ConsumersManager:

    """Consumers manager.

    Container for all application consumers except cluster.
    """

    event_consumers: Dict
    message_consumers: Dict
    output_consumers: Dict

    generation_consumer: GenerationConsumer
    generation_listener: asyncio.Task

    def __init__(self, app):
        self.app = app

        self.event_consumers = {}
        self.message_consumers = {}
        self.output_consumers = defaultdict(dict)

    async def start_all(self):
        """Start all common consumers.
        """
        await self.create_event_consumers()
        await self.create_message_consumers()
        await self.create_output_consumers()

        self.generation_listener = self.app.loop.create_task(
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
        """Create consumers for each event type.
        """
        for event_type in self.app.event_types():
            event_pipeline = self.app.config.get_event_pipeline(event_type)
            generators = self.app.config.get_generators(event_type)

            self.event_consumers[event_type] = EventConsumer(
                event_type=event_type,
                event_pipeline=event_pipeline,
                generators=generators,
                cluster=self.app.cluster,
                queue=await self.app.queue.events_queue(event_type),
                # TODO: replace with tmp queue factory?
                queue_service=self.app.queue,
                loop=self.app.loop,
            )
            await self.event_consumers[event_type].start()

    async def create_message_consumers(self):
        """Create message consumers.
        """

        for event_type in self.app.event_types():
            self.app.log.debug("Create MessageConsumer for type %s", event_type)

            self.message_consumers[event_type] = MessageConsumer(
                event_type,
                router=self.app.get_router(event_type),
                output_queue=await self.app.queue.output_queue(event_type),
                available_outputs=self.app.config.get_enabled_outputs(event_type),
                queue=await self.app.queue.messages_queue(event_type),
                loop=self.app.loop
            )
            await self.message_consumers[event_type].start()

    async def create_output_consumers(self):
        """Create output consumers.
        """
        for event_type in self.app.event_types():
            for output in self.app.config.get_enabled_outputs(event_type):
                queue = await self.app.queue.output_queue(event_type, output)
                messages_queue = await self.app.queue.messages_queue(
                    event_type
                )
                self.output_consumers[output][event_type] = OutputConsumer(
                    router=self.app.get_router(event_type),
                    event_type=event_type,
                    messages_queue=messages_queue,
                    queue=queue,
                    loop=self.app.loop
                )
                await self.output_consumers[output][event_type].start()

    async def listen_generation(self):
        """Listen generation queue of cluster for queue names to consume.

        TODO: rename

        Creates consumer for generated messages when cluster event received.
        """
        self.app.log.debug("Listen clusters generation queue")

        messages_queue = await self.app.queue.messages_queue(
            'example_event'
        )
        self.generation_consumer = GenerationConsumer(
            messages_queue=messages_queue, loop=self.app.loop
        )
        await self.generation_consumer.start()

        while True:
            queue_name = await self.app.cluster.generation_queue.get()
            self.app.log.debug('Message in generation_queue %s', queue_name)
            queue = await self.app.queue.generation_queue(name=queue_name)
            self.generation_consumer.consume(queue)

    async def stop_listen_generation(self):
        """Stop listen for generation queues.
        """
        await self.generation_consumer.stop()
        self.generation_listener.cancel()


async def stop_all(consumers):
    """Stop all consumers.
    """
    # pylint: disable=expression-not-assigned
    [await a.stop() for a in consumers.values()]
