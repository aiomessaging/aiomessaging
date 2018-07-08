from aiomessaging.event import Event, event_from_dict


def test_simple():
    e1 = Event('echo')
    e2 = event_from_dict({'type': 'echo'})
    assert e1.type == e2.type


def test_to_dict():
    e = Event('echo')
    e.to_dict()
