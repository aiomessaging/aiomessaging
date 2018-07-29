"""Output consumer.
"""
from ..message import Message
from ..router import Router

from .base import BaseMessageConsumer


class OutputConsumer(BaseMessageConsumer):
    """Output consumer.
    """
    event_type: str
    router: Router

    def __init__(self, event_type: str, router: Router, **kwargs) -> None:
        super().__init__(**kwargs)
        self.event_type = event_type
        self.router = router

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
        self.router.apply_next_effect(message)
        # TODO: this is actual end of pipeline. we need to reschedule message
        #       if next route exists.
        message.log.debug("Message in output handler "
                          "[this is the end for a while]")
