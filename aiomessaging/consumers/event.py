"""Event consumer.
"""
from typing import Callable

from ..event import Event
# from ..exceptions import DropException, DelayException

from .base import SingleQueueConsumer


class EventConsumer(SingleQueueConsumer):

    """Event consumer.

    Receive messages from inbound queue, pass it though event pipeline and
    generate messages using generation pipeline.
    """

    queue_prefix = "aiomessaging.events"
    generation_complete_handler: Callable

    def __init__(self, event_type, event_pipeline, generators,
                 queue_service, **kwargs):
        super().__init__(**kwargs)
        self.event_type = event_type
        self.pipeline = event_pipeline
        self.generators = generators
        self.queue_service = queue_service

    def on_generation_complete(self, handler):
        """Add generation complete callback.

        Handler will be invoked after generation complete and will receive tmp
        queue name in `queue_name` argument.
        """
        self.generation_complete_handler = handler

    async def handler(self, message):
        event = Event('example_event', payload=message)
        event.log.info("Event received")
        try:
            await self.handle_event(event)
        # except DropException:
        #     pass
        # except DelayException:
        #     pass
        except Exception:  # pylint: disable=broad-except
            self.log.exception("Exception in event handler")

    async def handle_event(self, event: Event):
        """Event handler.

        Process event with event pipeline and pass it to generator.
        """
        event = await self.pipeline(event)
        await self.generate_messages(event)

    async def generate_messages(self, event: Event):
        """Generate messages from event.

        Start generators and pass tmp queue to them. Wait them to finish.
        """
        event.log.info("Start generation")
        tmp_queue = await self.queue_service.generation_queue(self.event_type)
        await self.generators(tmp_queue, event)
        # TODO: check generator results. Stop if failed.
        await self.start_consume(tmp_queue)
        event.log.info("Generation finished")

    async def start_consume(self, queue):
        """ Start consume queue with generated messages
        """
        if hasattr(self, 'generation_complete_handler'):
            await self.generation_complete_handler(queue.name)
