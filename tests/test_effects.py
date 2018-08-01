from aiomessaging.message import Message
from aiomessaging.effects import SendEffect
from aiomessaging.actions import SendOutputAction

from .tmp import DeliveryBackend


def test_send_simple():
    effect = SendEffect(DeliveryBackend())
    message = Message(id='test_send_simple', event_type="test_event")
    assert isinstance(effect.next_action(), SendOutputAction)
    state = effect.apply(message)
    assert effect.next_action(state) is None
