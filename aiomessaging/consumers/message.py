"""Message consumer.
"""
from ..message import Message

from .base import BaseMessageConsumer


class MessageConsumer(BaseMessageConsumer):

    """Delivery consumer.

    Recieve message from output queue, send it to selected output (event
    type + backend combination).
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
        queue = self.output_queue
        self.log.debug("Send message to %s: %s", queue.name, message)
        await queue.publish(message.to_dict())
        queue.close()
