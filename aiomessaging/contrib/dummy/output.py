"""
Dummy output backends.

Used for testing the most.
"""
import json
import logging

from aiomessaging.message import Message
from aiomessaging.outputs import AbstractOutputBackend
from aiomessaging.exceptions import CheckDelivery, Retry


class NullOutput(AbstractOutputBackend):

    """Send messages to nowhere.

    Can be used to measure clean messaging performance with no regard to output
    throughput.
    """

    name = 'null'

    def send(self, message: Message, retry=0):
        return True


class ConsoleOutput(AbstractOutputBackend):

    """Send messages to console/log.

    Will output messages to worker log (for debug proposes).
    """

    name = 'console'
    msg_prefix = "Message delivered"

    def send(self, message: Message, retry=0):
        formatted_message = json.dumps(message.serialize(), indent=4)
        logging.info("%s:\n%s", self.msg_prefix, formatted_message)
        return True


class FailingOutput(AbstractOutputBackend):

    """Always failing output backend.

    This output will fail with exception for any incoming message.
    Used in tests.
    """

    name = 'failing'

    def send(self, message: Message, retry=0):
        raise Exception("FailingOutput fail (just test)")


class NeverDeliveredOutput(AbstractOutputBackend):

    """Output that never deliver message.

    `send` will always return false. Used in tests.
    """

    name = 'never'

    def send(self, message: Message, retry=0):
        return False


class CheckOutput(AbstractOutputBackend):

    """Send message and check delivery.

    Instead of simple forget message like NullOutput and mark it as delivered,
    this output will make additional cycle through messages queue to make a
    delivery check (which will be successful)
    """

    name = 'check'

    def send(self, message: Message, retry=0):
        """Always send to delivery check.
        """
        raise CheckDelivery()

    def check(self, message: Message):
        """Check is always successful.
        """
        return True


class RetryOutput(AbstractOutputBackend):

    """Retry sending until requested number of retries achieved.

    :param in retries: Number of retries to achieve.
    """

    name = 'retry'

    def send(self, message: Message, retry=0):
        """Send message with retry.
        """
        expected_retries = self.kwargs.get('retries', 1)
        if retry < expected_retries:
            raise Retry("Test retry")
