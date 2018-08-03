from aiomessaging.message import Message
from aiomessaging.effects import SendEffect, OutputStatus, reset_check_to_pending
from aiomessaging.actions import SendOutputAction

from .tmp import DeliveryBackend, FailingDeliveryBackend


def test_send_simple():
    effect = SendEffect(DeliveryBackend())
    message = Message(id='test_send_simple', event_type="test_event")
    assert isinstance(effect.next_action(), SendOutputAction)
    state = effect.apply(message)
    assert effect.next_action(state) is None


def test_failing_action():
    message = Message(id='test_send_simple', event_type="test_event")
    effect = SendEffect(FailingDeliveryBackend())
    effect.apply(message)


def test_reset_check_to_pending():
    state = [OutputStatus.CHECK, OutputStatus.CHECK]
    state = reset_check_to_pending(state)
    assert state[0] == OutputStatus.PENDING
    assert state[1] == OutputStatus.PENDING
