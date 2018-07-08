import logging
from typing import Dict, List

from .utils import gen_id
from .event import Event, event_from_dict


logger = logging.getLogger(__name__)


class Message:
    id = None
    event: Event = None
    content: Dict = {}
    meta: Dict = {}
    route: List = []

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
        return self.event.type

    def to_dict(self):
        return {
            'id': self.id,
            'event': self.event.to_dict(),
            'content': self.content,
            'meta': self.meta,
            'route': self.route
        }

    def __repr__(self):
        return "<Message:%s:id=%s>" % (self.event.type, self.id)


def message_from_dict(data: dict) -> Message:
    try:
        event = event_from_dict(data.pop('event'))
    except KeyError:
        logger.warning('Message without event received')
        return None
    if 'type' in data:
        data.pop('type')
    return Message(event=event, **data)
