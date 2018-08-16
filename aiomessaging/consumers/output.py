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

    def __init__(self, event_type: str, router: Router, messages_queue,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.event_type = event_type
        self.router = router
        self.messages_queue = messages_queue

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
        try:
            self.router.apply_next_effect(message)

            if self.router.next_effect(message):
                await self.messages_queue.publish(
                    message.serialize()
                )
                message.log.debug("Message rescheduled on message queue with "
                                  "queue_name=%s",
                                  self.messages_queue.name)
            else:
                message.log.info(
                    "Message has no next effect, delivery complete "
                    "[this is the end for a while]"
                )
                message.log.debug("Finish status:\n%s\n", message.pretty())
        # pylint:disable=broad-except
        except Exception:
            self.log.exception("Exception while routing message")
