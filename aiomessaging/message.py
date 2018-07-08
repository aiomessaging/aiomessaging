"""Message object.
"""
import logging
from typing import Dict, List

from .utils import gen_id
from .event import Event


logger = logging.getLogger(__name__)


class Message:
    """Message.
    """
    id = None
    event: Event
    content: Dict = {}
    meta: Dict = {}
    route: List = []

    # pylint: disable=redefined-builtin,too-many-arguments
    def __init__(self, id=None, event=None, content=None, meta=None,
                 route=None):
        if not event:
            raise Exception("No event!")
        self.id = id
        if self.id is None:
            self.id = gen_id(event.id)
        self.event = event
        self.content = content
        self.meta = meta
        self.route = route

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
