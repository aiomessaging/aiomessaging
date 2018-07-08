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
