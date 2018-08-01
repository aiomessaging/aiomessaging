from aiomessaging import Message, Route, Effect

from aiomessaging.effects import send

from .tmp import DeliveryBackend


def test_route():
    msg = Message(event_type='example_event', event_id='123')
    assert msg.route == []

    expected_output = DeliveryBackend()

    effect = send(expected_output)
    route = Route(effect=effect)


def test_route_serialize():
    expected_output = DeliveryBackend()
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
    message = Message(id='test_message', event_type='test_event')
    effect = send(DeliveryBackend())
    message.set_route_state(effect, {'a': 1})
    assert message.get_route_state(effect) == {'a': 1}


def test_set_status():
    message = Message(id='test_message', event_type='test_event')
    effect = send(DeliveryBackend())
    message.set_route_status(effect, 1)
    assert message.get_route_status(effect) == 1
