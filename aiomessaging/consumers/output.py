"""Output consumer.
"""
from ..message import Message

from .base import BaseMessageConsumer


class OutputConsumer(BaseMessageConsumer):
    """Output consumer.
    """
    async def handle_message(self, message: Message):
        """
        1. Try send message thought backend
        2. Handle exceptions from backend:
            a. CheckException(delay=X) — send message to delay(X) queue
            b. Retry(delay=X) - send message to delay(X) queue
            c. ConditionalStop(status=Y) — set message status Y and stop
               delivery
            d. NeverDelivered - mark backend as failed and send message back
               to output queue (select next backend in next step)
        """
        self.log.info("Output is here!!! %s", message)
