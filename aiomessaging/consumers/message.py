"""Message consumer.
"""
from ..message import Message
from ..router import Router

from .base import BaseMessageConsumer


class MessageConsumer(BaseMessageConsumer):

    """Message consumer.

    Consume messages from `messages.<event_type>` queue and route it to the
    next output(s).

    Output queue used to distribute message delivery between all subscribed
    workers.
    """

    router: Router

    def __init__(self, event_type, router: Router, output_queue,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.event_type = event_type
        self.router = router
        self.output_queue = output_queue

    async def handle_message(self, message: Message):
        """Message handler.

        Select next output for message and send it to related queue.
        """
        try:
            output = self.router.next_output(message)
            await self.output_queue.publish(
                message.to_dict(), routing_key=output.name
            )
            message.log.debug("published to output %s, routing_key=%s",
                              self.output_queue.name, output.name)
        # pylint: disable=broad-except
        except Exception:  # pragma: no cover
            message.log.exception("Unhandled exception in MessageConsumer")
