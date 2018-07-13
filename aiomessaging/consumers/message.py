"""Message consumer.
"""
from ..message import Message

from .base import BaseMessageConsumer


class MessageConsumer(BaseMessageConsumer):

    """Message consumer.

    Consume messages from `messages.<event_type>` queue and route it to the
    next output(s).
    """

    def __init__(self, event_type, router, output_queue, **kwargs):
        super().__init__(**kwargs)
        self.event_type = event_type
        self.router = router
        self.output_queue = output_queue

    async def handle_message(self, message: Message):

        """
        TODO:
        1. Select backend
        2a. Log if message not delivered
        2b. Send message to delivery queue of selected backend
        """
        # output = self.router.select_output(self.event_type, message)
        await self.output_queue.publish(message.to_dict())
        message.log.debug("published to output")
