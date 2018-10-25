"""Cluster utils.
"""
from typing import Dict, List, Callable
from collections import defaultdict

from .queues import AbstractQueue
from .consumers.base import SingleQueueConsumer
from .utils import class_from_string


class Cluster(SingleQueueConsumer):

    """Cluster.

    Provide interface to other cluster instances and allows to add handlers for
    specific actions. When action received from cluster queue, all related
    handlers will be invoked. All errors will be ignored (just logged).
    """

    START_CONSUME = 'start_consume'
    OUTPUT_OBSERVED = 'output_observed'

    ACTIONS = {START_CONSUME, OUTPUT_OBSERVED}

    action_handlers: Dict[str, List[Callable]]

    def __init__(self, queue: AbstractQueue, loop=None, **kwargs):
        self.action_handlers = defaultdict(list)

        super().__init__(queue=queue, loop=loop, **kwargs)

    async def start_consume(self, queue_name):
        """Publish message to cluster to start consume queue with generated messages.
        """
        await self.queue.publish(
            {'action': self.START_CONSUME, 'queue_name': queue_name}
        )
        self.log.debug("tell cluster to start consume %s", queue_name)

    def on_start_consume(self, handler):
        """Add handler for START_CONSUME shortcut.
        """
        self.add_action_handler(self.START_CONSUME, handler)

    async def output_observed(self, event_type, output):
        """Publish to cluster new output backends.
        """
        await self.queue.publish({
            'action': self.OUTPUT_OBSERVED,
            'event_type': event_type,
            'output': output.serialize()
        })

    def on_output_observed(self, handler):
        """Add handler for OUTPUT_OBSERVED shortcut.
        """
        def wrapper(event_type, output):
            # deserialize output instance
            cls_path, args, kwargs = output
            OutputCls = class_from_string(cls_path)
            return handler(event_type, OutputCls.load(args, kwargs))

        self.add_action_handler(self.OUTPUT_OBSERVED, wrapper)

    async def handler(self, message):
        """Handle cluster message.

        Get message from cluster queue, check if it contain `action` field and
        this action is available, invoke all handlers bonded to this action.
        """
        self.log.debug("Message body:\n%s", message)

        try:
            cluster_action = message.pop('action')
        except KeyError:
            self.log.error("No action in cluster message: %s", message)
            return

        if cluster_action not in self.ACTIONS:
            self.log.error("Invalid cluster action %s", cluster_action)
            return

        for handler in self.action_handlers[cluster_action]:
            try:
                await handler(**message)
            # pylint: disable=broad-except
            except Exception:  # pragma: no cover
                self.log.exception(
                    "Exception while handling cluster action %s (%s)",
                    cluster_action, handler
                )

    def add_action_handler(self, action, handler):
        """Add handler for specified action.
        """
        assert action in self.ACTIONS
        self.action_handlers[action].append(handler)
