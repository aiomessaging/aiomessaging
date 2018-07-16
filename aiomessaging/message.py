"""Message object.
"""
import logging
from typing import Dict, List, Optional

from .utils import gen_id
from .logging import MessageLoggerAdapter


logger = logging.getLogger(__name__)


class Message:
    """Message.
    """
    __slots__ = ['id', 'event_type', 'content', 'meta', 'route', 'log']

    id: int
    event_type: str
    content: Optional[Dict]
    meta: Optional[Dict]
    route: Optional[List]
    log: MessageLoggerAdapter

    # pylint: disable=redefined-builtin,too-many-arguments
    def __init__(self, id=None, event_id=None, event_type=None, content=None,
                 meta=None, route=None):
        if not event_type:
            raise Exception("Message constuctor requires event_type kwarg")
        if not event_id and not id:
            raise Exception("Message constuctor requires event_id or id kwarg")

        self.id = id or gen_id(event_id)
        self.event_type = event_type
        self.content = content or {}
        self.meta = meta
        self.route = route
        self.log = MessageLoggerAdapter(self)

    @property
    def type(self):
        """Message event type.
        """
        return self.event_type

    def to_dict(self) -> dict:
        """Serialize message to dict.
        """
        return {
            'id': self.id,
            'event_type': self.event_type,
            'content': self.content,
            'meta': self.meta,
            'route': self.route
        }

    @staticmethod
    def from_dict(data: dict) -> 'Message':
        """Load message from provided dict.
        """
        if 'type' in data:
            data['event_type'] = data.pop('type')
        return Message(**data)

    def __repr__(self):
        """Instance representation.
        """
        return "<Message:%s:id=%s>" % (self.event_type, self.id)
