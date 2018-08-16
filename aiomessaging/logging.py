"""Logging helpers.
"""
import logging

from termcolor import colored

from .utils import short_id


class ConsumerLoggerAdapter(logging.LoggerAdapter):
    """Consumer logger adapter
    """
    def process(self, msg, kwargs):
        name = self.extra.get('name')
        return colored(f"[{name}] {msg}", attrs=['bold']), kwargs


class QueueLoggerAdapter(logging.LoggerAdapter):

    """Queue logger adapter.

    Queue name will be appended for every log message passed.
    """

    def __init__(self, logger, queue, color=True):
        super().__init__(logger, extra={})
        self.queue_name = queue.name
        self.color = color

    def process(self, msg, kwargs):
        prefix = f"[{self.queue_name}]"
        if self.color:
            prefix = colored(prefix, color="magenta")
        return ' '.join([prefix, msg]), kwargs


class MessageLoggerAdapter(logging.LoggerAdapter):

    """Message logger adapter.

    Message id will be appended for every log message passed.
    """

    def __init__(self, message, color=True):
        logger = logging.getLogger('aiomessaging.message')
        super().__init__(logger, extra={})
        self.message_id = message.id
        self.color = color

    def process(self, msg, kwargs):
        prefix = f"[{short_id(self.message_id, 8, 2)}]"
        if self.color:
            prefix = colored(prefix, color="cyan")
        return ' '.join([prefix, msg]), kwargs


class EventLoggerAdapter(logging.LoggerAdapter):

    """Event logger adapter.

    Event id will be appended for every log message passed.
    """

    def __init__(self, event, color=True):
        logger = logging.getLogger('aiomessaging.event')
        super().__init__(logger, extra={})
        self.event_id = event.id
        self.color = color

    def process(self, msg, kwargs):
        prefix = f"[{short_id(self.event_id)}]"
        if self.color:
            prefix = colored(prefix, color="blue")
        return ' '.join([prefix, msg]), kwargs
