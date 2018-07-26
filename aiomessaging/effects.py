"""
pipeline effects
"""
import sys
import abc
import enum

from typing import Dict, Optional

from .actions import Action, SendOutputAction

from .utils import NamedSerializable, class_from_string


_registered_effects: Dict = {}


def register_effect(effect_cls):
    """Register effect in library.

    Will raise exception if `name` already registered.
    """
    if effect_cls.name in _registered_effects:
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


def reset_check_to_pending(state):
    """Reset all check statuses to pending.

    Preserve all other statuses. Raise exception if pending found.
    """
    result = []
    for status in state:
        if status == OutputStatus.CHECK:
            result.append(OutputStatus.PENDING)
        elif status == OutputStatus.PENDING:
            raise Exception("Found pending when resetting CHECK")
        else:
            result.append(status)
    return result


class Effect(NamedSerializable, abc.ABC):

    """Abstract pipeline effect.

    Effect used in pipeline generator. Pipeline return effects instead of
    performing heavy operations. Any effect can be serialized, transferred to
    the place of execution.

    The method `next_action` must be implemented on all derived classes and
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


@register_effect
class SendEffect(Effect):
    """Effect: send message through outputs.

    Accepts outputs as the args.
    """
    name = 'send'

    def next_action(self, state):
        if state is None:
            # create default state with all backends pending
            state = [OutputStatus.PENDING for b in self.args]

        assert len(state) == len(self.args), "State and args length must match"

        # check if no pending in state
        pendings = [s for s in state if s == OutputStatus.PENDING]
        if not pendings:
            state = reset_check_to_pending(state)

        output = None
        # search next pending backend
        for output, status in zip(self.args, state):
            if status == OutputStatus.PENDING:
                break
        return SendOutputAction(output)

    def serialize_args(self):
        return [b.serialize() for b in self.args]

    @classmethod
    def load_args(cls, args):
        return [get_class_instance(*b) for b in args]


# @register_effect
# class CallEffect(Effect):

#     name = 'call'

#     def next_action(self, state):
#         """Execute callable in message consumer.
#         """
#         return CallAction(self.args[0], *self.args[1:], **self.kwargs)


send = SendEffect
# call = CallEffect


for name, effect in _registered_effects.items():
    setattr(sys.modules[__name__], name, effect)
