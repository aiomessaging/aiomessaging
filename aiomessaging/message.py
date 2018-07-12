"""Message object.
"""
import logging
from typing import Dict, List

from .utils import gen_id
from .event import Event
from .logging import MessageLoggerAdapter


logger = logging.getLogger(__name__)


class Message:
    """Message.
    """
    __slots__ = ['id', 'event', 'content', 'meta', 'route', 'log']
    id: int
    event: Event
    content: Dict
    meta: Dict
    route: List
    log: MessageLoggerAdapter

    # pylint: disable=redefined-builtin,too-many-arguments
    def __init__(self, id=None, event=None, content=None, meta=None,
                 route=None):
        if not event:
            raise Exception("No event provided for message!")

        self.id = id or gen_id(event.id)
        self.event = event
        self.content = content or {}
        self.meta = meta
        self.route = route
        self.log = MessageLoggerAdapter(self)

    @property
    def type(self):
        """Message event type.
        """
        return self.event.type

    def to_dict(self) -> dict:
        """Serialize message to dict.
        """
        return {
            'id': self.id,
            'event': self.event.to_dict(),
            'content': self.content,
            'meta': self.meta,
            'route': self.route
        }

    @staticmethod
    def from_dict(data: dict) -> 'Message':
        """Load message from provided dict.
        """
        try:
            event = Event.from_dict(data.pop('event'))
        except KeyError:
            logger.warning('Message without event received')
            raise Exception("Message without event received")
        if 'type' in data:
            data.pop('type')
        return Message(event=event, **data)

    def __repr__(self):
        """Instance representation.
        """
        return "<Message:%s:id=%s>" % (self.event.type, self.id)
