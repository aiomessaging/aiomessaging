"""
Dummy output backends.

Used for testing the most.
"""
import json
import logging

from aiomessaging.message import Message
from aiomessaging.outputs import AbstractOutputBackend
from aiomessaging.exceptions import CheckDelivery


class NullOutput(AbstractOutputBackend):

    """Send messages to nowhere.

    Can be used to measure clean messaging performance with no regard to output
    throughput.
    """

    name = 'null'

    def send(self, message: Message):
        return True


class ConsoleOutput(AbstractOutputBackend):

    """Send messages to console/log.

    Will output messages to worker log (for debug proposes).
    """

    name = 'console'
    msg_prefix = "Message delivered"

    def send(self, message: Message):
        formatted_message = json.dumps(message.serialize(), indent=4)
        logging.info("%s:\n%s", self.msg_prefix, formatted_message)
        return True


class FailingOutput(AbstractOutputBackend):

    """Always failing output backend.

    This output will fail with exception for any incoming message.
    Used in tests.
    """

    name = 'failing'

    def send(self, message: Message):
        raise Exception("FailingOutput fail (just test)")


class NeverDeliveredOutput(AbstractOutputBackend):

    """Output that never deliver message.

    `send` will always return false. Used in tests.
    """

    name = 'never'

    def send(self, message: Message):
        return False


class CheckOutput(AbstractOutputBackend):

    """Send message and check delivery.

    Instead of simple forget message like NullOutput and mark it as delivered,
    this output will make additional cycle through messages queue to make a
    delivery check (which will be successful)
    """

    name = 'check'

    def send(self, message: Message):
        """Always send to delivery check.
        """
        raise CheckDelivery()

    def check(self, message: Message):
        """Check is always successful.
        """
        return True
