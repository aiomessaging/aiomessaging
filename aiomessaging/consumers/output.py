"""Output consumer.
"""
from ..message import Message

from .base import BaseMessageConsumer


class OutputConsumer(BaseMessageConsumer):
    """Output consumer.
    """
    def __init__(self, event_type, **kwargs):
        super().__init__(**kwargs)
        self.event_type = event_type

    async def handle_message(self, message: Message):
        """
        1. Try send message through backend
        2. Handle exceptions from backend:
            a. CheckException(delay=X) — send message to delay(X) queue
            b. Retry(delay=X) - send message to delay(X) queue
            c. ConditionalStop(status=Y) — set message status Y and stop
               delivery
            d. NeverDelivered - mark backend as failed and send message back
               to output queue (select next backend in next step)
        """
        # TODO: this is actual end of pipeline
        message.log.debug("Message in output handler "
                          "[this is the end for a while]")
