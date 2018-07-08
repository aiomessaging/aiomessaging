"""Message consumer.
"""
from ..message import Message

from .base import BaseMessageConsumer


class MessageConsumer(BaseMessageConsumer):

    """Delivery consumer.

    Recieve message from output queue, send it to selected output (event
    type + backend combination).
    """

    def __init__(self, event_type, *args, router, output_queue, **kwargs):
        super().__init__(event_type, *args, **kwargs)
        self.router = router
        self.output_queue = output_queue
        self.log.info("MessageConsumer started to consume %s", self.queue)

    async def handle_message(self, message: Message):

        """
        TODO:
        1. Select backend
        2a. Log if message not delivered
        2b. Send message to delivery queue of selected backend
        """
        # output = self.router.select_output(self.event_type, message)
        queue = self.output_queue
        self.log.debug("Delivery message to %s: %s", queue, message)
        await queue.publish(message.to_dict())
        queue.close()
