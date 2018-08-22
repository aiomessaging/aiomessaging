"""Message consumer.
"""
from ..message import Message
from ..router import Router

from .base import BaseMessageConsumer
from ..actions import SendOutputAction, CheckOutputAction


class OutputNotAvailable(Exception):
    """Output not available exception.

    Raised if output returned from pipeline is not available (no consumers for
    such routing key, message will not be delivered).
    """
    pass


class MessageConsumer(BaseMessageConsumer):

    """Message consumer.

    Consume messages from `messages.<event_type>` queue and route it to the
    next output(s).

    Output queue used to distribute message delivery between all subscribed
    workers.
    """

    router: Router

    def __init__(self, event_type, router: Router, output_queue,
                 available_outputs, **kwargs) -> None:
        super().__init__(**kwargs)
        self.event_type = event_type
        self.router = router
        self.output_queue = output_queue
        self.available_outputs = available_outputs

    async def handle_message(self, message: Message):
        """Message handler.

        Select next output for message and send it to related queue.
        """
        try:
            while True:
                effect = self.router.next_effect(message)
                if effect is None:
                    # TODO: implement same logic as message delivered in output
                    #       consumer
                    message.log.warning(
                        "No next effect for message (in message consumer)"
                    )
                    break
                prev_state = message.get_route_state(effect)
                action = effect.next_action(prev_state)

                if isinstance(action, (SendOutputAction, CheckOutputAction)):
                    # send message to output queue
                    output = action.get_output()

                    if output.name not in self.available_outputs:
                        self.router.skip_next_effect(message)
                        message.log.error(
                            "Output %s skipped because it is not available. "
                            "Available outputs: %s",
                            output.name, self.available_outputs
                        )
                        continue

                    await self.output_queue.publish(
                        message.to_dict(), routing_key=output.name
                    )
                    message.log.debug("published to output %s, routing_key=%s",
                                      self.output_queue.name, output.name)
                    # TODO: publish not confirmed
                    return True

                message.log.error("Unhandled action type %s", type(action))  # pragma: no cover
        # pylint: disable=broad-except
        except Exception:  # pragma: no cover
            message.log.exception("Unhandled exception in MessageConsumer")
