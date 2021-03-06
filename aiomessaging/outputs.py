"""
Output backend abstraction and general implementation.
"""
from abc import ABC, abstractmethod
from typing import List, Dict

from .message import Message
from .utils import Serializable


class NoDeliveryCheck(Exception):
    """Backend has no delivery check exception.

    Raised from output backends `send` method if no delivery check applied to
    this backend.

    It is not builtin `NotImplemented` because pylint thought that it is
    abstract and mark all child classes with error.
    """
    pass


class AbstractOutputBackend(ABC, Serializable):

    """Abstract output backend.

    Defines public api for backend and allows to dump and restore of backend
    instance in simple case.
    """

    name: str

    args: List
    kwargs: Dict

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

        assert self.name, "The name must be defined on output backend"

    @abstractmethod
    def send(self, message: Message, retry=0):
        """Send message through this backend.

        Must be implemented for every backend.

        :param Message message: message to send
        :param int retry: retry number (0 by default)
        """
        pass  # pragma: no cover

    # pylint:disable=no-self-use
    def check(self, message: Message):
        """Check delivery status for message.

        Can raise `NoDeliveryCheck` if backend doesn't support delivery check.
        """
        raise NoDeliveryCheck  # pragma: no cover
