"""Event consumer.
"""
from ..event import Event
from ..exceptions import DropException, DelayException

from .base import SingleQueueConsumer


class EventConsumer(SingleQueueConsumer):

    """Event consumer.

    Recieve messages from inbound queue, pass it though event pipeline,
    generate messages using generation pipeline.
    """

    queue_prefix = "aiomessaging.events"

    def __init__(self, event_type, event_pipeline, generators, cluster,
                 queue_service, **kwargs):
        super().__init__(**kwargs)
        self.event_type = event_type
        self.pipeline = event_pipeline
        self.generators = generators
        self.cluster = cluster
        self.queue_service = queue_service

    async def handler(self, message):
        self.log.debug("message received: %s", message)
        event = message
        try:
            await self.handle_event(event)
        except DropException:
            pass
        except DelayException:
            pass

    async def handle_event(self, event: Event):
        """Event handler.

        Process event with event pipeline and pass it to generator.
        """
        event = await self.pipeline(event)
        await self.generate_messages(event)

    async def generate_messages(self, event: Event):
        """Generate messages from event.
        """
        # start generators and pass tmp queue to them
        # wait them to finish
        self.log.debug("Generate messages for event %s", event)
        tmp_queue = await self.queue_service.generation_queue(self.event_type)
        self.log.debug('Tmp queue: %s', tmp_queue)
        await self.generators(tmp_queue, event)
        # TODO: check generator results. Stop if failed.
        await self.start_consume(tmp_queue)
        tmp_queue.close()

    async def start_consume(self, queue):
        """ Start consume queue with generated messages
        """
        await self.cluster.start_consume(queue.name)
