"""Message consumer.
"""
import asyncio

from typing import Callable

from ..message import Message
from ..router import Router
from ..queues import AbstractQueue
from ..actions import SendOutputAction, CheckOutputAction

from .base import BaseMessageConsumer


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

    event_type: str
    router: Router
    output_queue: AbstractQueue
    output_observed_handler: Callable

    def __init__(self, event_type, router: Router, output_queue,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.event_type = event_type
        self.router = router
        self.output_queue = output_queue

    def on_output_observed(self, handler):
        """Set output observed handler.
        """
        self.output_observed_handler = handler

    async def handle_message(self, message: Message):
        """Message handler.

        Select next output for message and send it to related queue.
        """
        try:
            while True:
                effect = self.router.next_effect(message)
                prev_state = message.get_route_state(effect)
                action = effect.next_action(prev_state)

                if isinstance(action, (SendOutputAction, CheckOutputAction)):
                    # send message to output queue
                    output = action.get_output()

                    # manager will create output consumer for us if possible
                    if hasattr(self, 'output_observed_handler'):
                        await self.output_observed_handler(self.event_type, output)

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
