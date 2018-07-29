"""Message consumer.
"""
from ..message import Message
from ..router import Router

from .base import BaseMessageConsumer
from ..actions import SendOutputAction


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
            while True:
                effect = self.router.next_effect(message)
                prev_state = message.get_route_state(effect)
                action = effect.next_action(prev_state)

                if action is None:
                    # No more effects and actions available, mark message as
                    # delivered and send stats.
                    message.log.info("End of delivery pipeline")
                    return

                if isinstance(action, SendOutputAction):
                    # send message to output queue
                    output = action.get_output()

                    await self.output_queue.publish(
                        message.to_dict(), routing_key=output.name
                    )
                    message.log.debug("published to output %s, routing_key=%s",
                                      self.output_queue.name, output.name)
                    # TODO: publish not confirmed
                    return True

                message.log.error("Unhandled action type %s", type(action))
        # pylint: disable=broad-except
        except Exception:  # pragma: no cover
            message.log.exception("Unhandled exception in MessageConsumer")
