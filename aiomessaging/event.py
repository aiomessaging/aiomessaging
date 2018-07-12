"""Event object.
"""
from .logging import EventLoggerAdapter

from .utils import gen_id


class Event:
    """Event.

    Represents incoming message.
    """
    def __init__(self, event_type, payload=None, event_id=None):
        self.id = event_id
        if self.id is None:
            self.id = gen_id()

        self.type = event_type
        self.payload = payload or {}

        self.log = EventLoggerAdapter(self)

    def to_dict(self):
        """Serialize event to dict.
        """
        return {
            'id': self.id,
            'type': self.type,
            'payload': self.payload
        }

    @staticmethod
    def from_dict(data: dict) -> 'Event':
        """Create Event from dict.
        """
        event_id = data.pop('id', None)
        event_type = data.pop('type')
        return Event(event_type, data, event_id)
