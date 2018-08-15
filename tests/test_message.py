"""
Message class tests.
"""
from aiomessaging import Message, Route, Effect
from aiomessaging.effects import send
from aiomessaging.contrib.dummy import NullOutput


def test_message_repr():
    msg = Message(event_type='example_event', event_id='123')
    repr(msg)


def test_route():
    msg = Message(event_type='example_event', event_id='123')
    assert msg.route == []

    expected_output = NullOutput()

    effect = send(expected_output)
    route = Route(effect=effect)

    # FIXME: incomplete


def test_route_serialize():
    """Test route serialization.
    """
    expected_output = NullOutput()
    effect = send(expected_output)
    route = Route(effect=effect)

    msg = Message.from_dict({
        'event_type': 'example_event',
        'event_id': '123',
        'route': [
            route.serialize()
        ]
    })

    assert msg.route[0].serialize() == route.serialize()


def test_update_state():
    """Test route state update.
    """
    message = Message(id='test_message', event_type='test_event')
    effect = send(NullOutput())
    message.set_route_state(effect, {'a': 1})
    assert message.get_route_state(effect) == {'a': 1}


def test_set_status():
    """Test route status update.
    """
    message = Message(id='test_message', event_type='test_event')
    effect = send(NullOutput())
    message.set_route_status(effect, 1)
    assert message.get_route_status(effect) == 1
