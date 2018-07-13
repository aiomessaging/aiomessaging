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

    # pylint: disable=too-many-arguments
    def __init__(self, event_type, event_pipeline, generators, cluster,
                 queue_service, **kwargs):
        super().__init__(**kwargs)
        self.event_type = event_type
        self.pipeline = event_pipeline
        self.generators = generators
        self.cluster = cluster
        self.queue_service = queue_service

    async def handler(self, message):
        event = Event('example_event', payload=message)
        event.log.info("Event in event consumer")
        try:
            await self.handle_event(event)
        except DropException:
            pass
        except DelayException:
            pass
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
        await self.cluster.start_consume(queue.name)
