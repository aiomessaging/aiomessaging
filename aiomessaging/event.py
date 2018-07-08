from .utils import gen_id


class Event(object):
    def __init__(self, event_type, payload=None, event_id=None):
        self.id = event_id
        self.type = event_type
        self.payload = payload or {}

        if self.id is None:
            self.id = gen_id()

    def to_dict(self):
        return {
            'id': self.id,
            'type': self.type,
            'payload': self.payload
        }


def event_from_dict(data: dict) -> Event:
    event_id = data.pop('id', None)
    event_type = data.pop('type')
    return Event(event_type, data, event_id)
