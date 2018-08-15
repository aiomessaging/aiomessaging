"""
Output pipeline effects test.
"""
import pytest
import logging

from aiomessaging.message import Message
from aiomessaging.effects import (
    SendEffect,
    OutputStatus,
)
from aiomessaging.actions import SendOutputAction, CheckOutputAction

from aiomessaging.contrib.dummy import (
    NullOutput,
    FailingOutput,
    CheckOutput,
    NeverDeliveredOutput,
)


logging.getLogger('aiomessaging').setLevel(logging.DEBUG)
logging.getLogger('aiomessaging.utils').setLevel(logging.INFO)


def test_send_simple():
    """Check that SendEffect returns provided output.
    """
    effect = SendEffect(NullOutput())
    message = Message(id='test_send_simple', event_type="test_event")
    assert isinstance(effect.next_action(), SendOutputAction)
    state = effect.apply(message)
    assert effect.next_action(state) is None


def test_failing_action():
    """Test that failing output properly handled.
    """
    message = Message(id='test_send_simple', event_type="test_event")
    effect = SendEffect(FailingOutput())
    with pytest.raises(Exception):
        effect.apply(message)


def test_never_delivered():
    """Test send through NeverDeliveredOutput
    """
    message = Message(id='test_send_simple', event_type="test_event")
    effect = SendEffect(NeverDeliveredOutput())
    effect.apply(message)

def test_next_action(caplog):
    message = Message(id='test_send_simple', event_type="test_event")
    effect = SendEffect(CheckOutput())
    state = effect.apply(message)
    assert state == [OutputStatus.CHECK]
    message.set_route_state(effect, state)
    action = effect.next_action(state)
    assert isinstance(action, CheckOutputAction)
    state = effect.apply(message)
    assert state == [OutputStatus.SUCCESS]
