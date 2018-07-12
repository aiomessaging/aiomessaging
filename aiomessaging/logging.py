"""Logging helpers.
"""
import logging

from termcolor import colored


class LoggerAdapter(logging.LoggerAdapter):
    """Message logger adapter.

    Allows to output information like event or message id with every log
    message.
    """
    def process(self, msg, kwargs):
        name = self.extra.get('name')
        event = self.extra.get('event')
        message = self.extra.get('message')

        log = []

        if name:
            log.append(colored(name, attrs=['bold']))

        if event and not message:
            log.append("[%s]" % event.id)
        elif message:
            log.append("[%s]" % message.id)

        log.append(msg)
        return ' '.join(log), kwargs


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


def short_id(some_id, length=8, right_add=0, sep='..'):
    half = int(length / 2)
    return sep.join([
        some_id[:half],
        some_id[-half-right_add:]
    ])
