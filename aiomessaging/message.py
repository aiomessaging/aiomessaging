"""Message object.
"""
import logging
from typing import Dict, List, Optional, Any

from .effects import Effect, EffectStatus, load_effect
from .utils import gen_id
from .logging import MessageLoggerAdapter


logger = logging.getLogger(__name__)

# Output pipeline effect statuses
ST_NEW = 0  # not started yet
ST_PENDING = 1  # started, wait check etc
ST_APPLIED = 2  # success
ST_FAILED = 3  # fail


class Message:

    """Message.
    """

    __slots__ = ['id', 'event_type', 'content', 'meta', 'route', 'log']

    id: int
    event_type: str
    content: Dict
    meta: Optional[Dict]
    route: List['Route']
    log: MessageLoggerAdapter

    # pylint: disable=redefined-builtin,too-many-arguments
    def __init__(self, id=None, event_id=None, event_type=None, content=None,
                 meta=None, route=None):

        if not event_type:  # pragma: no cover
            raise Exception("Message constructor requires event_type kwarg")

        if not event_id and not id:  # pragma: no cover
            raise Exception(
                "Message constructor requires event_id or id kwarg"
            )

        self.id = id or gen_id(event_id)
        self.event_type = event_type
        self.content = content or {}
        self.meta = meta
        self.route = route or []
        self.log = MessageLoggerAdapter(self)

    @property
    def type(self):
        """Message event type.
        """
        return self.event_type

    def get_route_status(self, effect):
        """Get actual status of effect.

        Return ST_NEW, ST_PENDING, ST_APPLIED, ST_FAILED.
        """
        for route in self.route:
            if route.effect == effect:
                return route.status
        return None

    def set_route_status(self, effect, status):
        """Set effect status.
        """
        for route in self.route:
            if route.effect == effect:
                route.status = status
                break
        else:
            self.route.append(Route(effect, status))

    def get_route_state(self, effect):
        """Get actual status of effect.

        Return ST_NEW, ST_PENDING, ST_APPLIED, ST_FAILED.
        """
        for route_effect, state, _ in self.route:
            if route_effect == effect:
                return state
        return None

    def to_dict(self) -> dict:
        """Serialize message to dict.
        """
        return {
            'id': self.id,
            'event_type': self.event_type,
            'content': self.content,
            'meta': self.meta,
            'route': [r.serialize() for r in self.route]
        }

    @staticmethod
    def from_dict(data: dict) -> 'Message':
        """Load message from provided dict.
        """
        if 'type' in data:
            data['event_type'] = data.pop('type')
        data['route'] = [
            Route.load(r) for r in data.get('route', [])
        ]
        return Message(**data)

    def __repr__(self):
        """Instance representation.
        """
        return "<Message:%s:id=%s>" % (self.event_type, self.id)


class Route:
    """Message route.

    Container for effect, its overall status and state.

    State may be any json-serializable object.
    """
    __slots__ = ['effect', 'status', 'state']

    effect: Effect
    status: EffectStatus
    state: Any

    def __init__(self,
                 effect: Effect,
                 status: EffectStatus = EffectStatus.PENDING,
                 state=None) -> None:
        self.effect = effect
        self.status = status
        self.state = state

    def serialize(self):
        """Serialize route.
        """
        return [self.effect.serialize(), self.status, self.state]

    @classmethod
    def load(cls, data) -> 'Route':
        """Load serialized route to Route object.
        """
        data[0] = load_effect(data[0])
        return cls(*data)
