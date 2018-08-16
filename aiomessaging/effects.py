"""
pipeline effects
"""
import sys
import abc
import enum
import logging

from itertools import zip_longest
from typing import Dict, Optional

from .actions import Action, SendOutputAction, CheckOutputAction
from .exceptions import CheckDelivery, Retry

from .utils import NamedSerializable, class_from_string


_registered_effects: Dict = {}

logger = logging.getLogger()


def register_effect(effect_cls):
    """Register effect in library.

    Will raise exception if `name` already registered.
    """
    if effect_cls.name in _registered_effects:  # pragma: no cover
        raise Exception(
            "Effect with name %s already registered" % effect_cls.name
        )
    _registered_effects[effect_cls.name] = effect_cls
    return effect_cls


def load_effect(data):
    """Load effect.
    """
    cls = _registered_effects[data[0]]
    return cls.load(data[1], data[2])


def get_class_instance(cls_name, args, kwargs):
    """Get instance from class name and arguments.
    """
    cls = class_from_string(cls_name)
    return cls(*args, **kwargs)


class EffectStatus(enum.Enum):
    """Route status.
    """
    PENDING = 1
    FINISHED = 2
    FAILED = 3


class OutputStatus(enum.Enum):
    """Output status for message.

    TODO: move to better place
    """
    PENDING = 1
    CHECK = 2
    SUCCESS = 3
    FAIL = 4
    RETRY = 5


class Effect(NamedSerializable, abc.ABC):

    """Abstract pipeline effect.

    Effect used in pipeline generator. Pipeline return effects instead of
    performing heavy operations. Any effect can be serialized, transferred to
    the place of execution.

    The method `next_effect` must be implemented on all derived classes and
    must return an `Action` instance or `None` if no next action available for
    this effect and it can be marked as applied by `MessageConsumer`.
    """

    name: str

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

        assert self.name, "Effect must define `name` property"

    @abc.abstractmethod
    def next_action(self, state) -> Optional[Action]:
        """Get next effect action.

        Receive state from last `apply` call.

        Return `Action` instance or `None` if no more actions available for
        this effect.
        """
        pass  # pragma: no cover

    @abc.abstractmethod
    def apply(self, message):
        """Apply next action and return next state.
        """
        pass  # pragma: no cover

    # pylint: disable=no-self-use
    def serialize_state(self, state):
        """Serialize effect state.
        """
        return state  # pragma: no cover

    def load_state(self, data):
        """Load serialized effect state.
        """
        return data  # pragma: no cover
    # pylint: enable=no-self-use

    def pretty(self):  # pragma: no cover
        """Pretty print effect.
        """
        return ''

    def __eq__(self, other):
        if not isinstance(other, self.__class__):  # pragma: no cover
            raise TypeError("Effect and %s can't be compared" % type(other))
        return self.serialize() == other.serialize()


@register_effect
class SendEffect(Effect):

    """Effect: send message through outputs.

    Accepts outputs as the args.
    """

    name = 'send'

    def next_action(self, state=None):
        """Next effect action.
        """
        state = self.reset_state(state)
        position = self.next_action_pos(state)

        if position is None:
            return None
        selected_output = self.args[position]

        if state[position] == OutputStatus.CHECK:
            return CheckOutputAction(selected_output)
        return SendOutputAction(selected_output)

    def next_action_pos(self, state):
        """Next effect action position.
        """
        state = self.reset_state(state, reset_pending=True)

        selected_output = None

        # search next pending backend
        for i, (_, status) in enumerate(zip(self.args, state)):
            if status == OutputStatus.PENDING:
                selected_output = i
                break
        else:
            for i, (_, status) in enumerate(zip(self.args, state)):
                if status == OutputStatus.CHECK:
                    selected_output = i
                    break
        return selected_output

    def reset_state(self, state, reset_pending=False):
        """Reset state.

        `reset_pending=True` force reset all RETRY to PENDING.

        TODO: also reset CHECK to CHECK_PENDING

        :params bool reset_pending: reset to reset_pending
        """
        if state is None or state == []:
            # create default state with all backends pending
            state = [OutputStatus.PENDING for b in self.args]

        assert len(state) == len(self.args), "State and args length must match"

        if reset_pending and OutputStatus.PENDING not in state:
            for i, status in enumerate(state):
                if status == OutputStatus.RETRY:
                    state[i] = OutputStatus.PENDING
        return state

    def apply(self, message):
        """Send message through next pending output.

        Modifies message route. Return state.
        """
        state = message.get_route_state(self)
        state = self.reset_state(state)

        position = self.next_action_pos(state)
        action = self.next_action(state)
        retry = message.get_route_retry(self)
        try:
            result = action.execute(message, retry)

            if result is False:  # ignore None
                state[position] = OutputStatus.FAIL
            else:
                state[position] = OutputStatus.SUCCESS
        except CheckDelivery:
            state[position] = OutputStatus.CHECK
        except Retry:
            prev = message.get_route_retry(self)
            message.set_route_retry(self, prev + 1)
            state[position] = OutputStatus.RETRY
            message.log.info("Delivery retried (%i)", prev + 1)

        return state

    def load_state(self, data):
        if not data:
            data = []
        return [OutputStatus(status) for status in data]

    def serialize_state(self, state):
        if not state:
            state = []
        return [status.value for status in state]

    def serialize_args(self):
        return [b.serialize() for b in self.args]

    @classmethod
    def load_args(cls, args):
        return [get_class_instance(*b) for b in args]

    def pretty(self, state):
        """Pretty format effect.
        """
        action_format = "{a.__class__.__name__} <{s}>"
        if not state:
            state = self.reset_state(state)
        return '\n\t\t\t'.join([
            action_format.format(a=a, s=s) for a, s in zip_longest(
                self.args, state
            )
        ])


# @register_effect
# class CallEffect(Effect):

#     name = 'call'

#     def next_effect(self, state):
#         """Execute callable in message consumer.
#         """
#         return CallAction(self.args[0], *self.args[1:], **self.kwargs)


send = SendEffect
# call = CallEffect


for name, effect in _registered_effects.items():
    setattr(sys.modules[__name__], name, effect)
