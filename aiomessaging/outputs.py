"""
Output backend abstraction and general implementation.
"""
from abc import ABC, abstractmethod

from .message import Message
from .utils import Serializable


class AbstractOutputBackend(ABC, Serializable):
    """Abstract output backend.

    Defines public api for backend and allows to dump and restore of backend
    instance in simple case.
    """
    name: str

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

        assert self.name, "The name must be defined on output backend"

    @abstractmethod
    def send(self, message: Message):
        """Send message through this backend.

        Must be implemented for every backend.
        """
        pass  # pragma: no cover

    @abstractmethod
    def check(self, message: Message):
        """Check delivery status for message.

        Can raise `NotImplemented()` if backend doesn't support delivery check.
        """
        raise NotImplementedError  # pragma: no cover


def _entity_to_string(entity):
    """Serialize module entity (class or function) to string.
    """
    return '.'.join([
        entity.__module__,
        entity.__name__
    ])
