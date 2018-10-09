"""
Consumers manager.
"""
from typing import Dict
from collections import defaultdict

from .event import EventConsumer
from .message import MessageConsumer
from .output import OutputConsumer


class ConsumersManager:

    """Consumers manager.

    Container for all application consumers.
    """
    app: 'Application'

    event_consumers: Dict
    message_consumers: Dict

    def __init__(self, app):
        self.app = app

        self.event_consumers = {}
        self.message_consumers = {}
        self.output_consumers = defaultdict(dict)

    async def start_all(self):
        await self.create_event_consumers()
        await self.create_message_consumers()
        await self.create_output_consumers()

    async def stop_all(self):
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
            self.app.log.debug("Create event consumer for type %s", event_type)

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


async def stop_all(consumers):
    """Stop all consumers.
    """
    # pylint: disable=expression-not-assigned
    [await a.stop() for a in consumers.values()]
