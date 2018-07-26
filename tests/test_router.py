"""
router test suite
"""
from aiomessaging.router import Router
from aiomessaging.message import Message
from aiomessaging.effects import send, SendEffect

from .tmp import DeliveryBackend


def simple_pipeline(message):
    """Simple pipeline.

    Send message through test delivery backend
    """
    yield send(DeliveryBackend())


def test_simple_pipeline():
    """Test router constructor and simple pipeline
    """
    router = Router(output_pipeline=simple_pipeline)
    message = Message(event_id='test_simple', event_type='example_event')
    result = router.next_output(message)

    assert isinstance(result, DeliveryBackend)


def test_send_effect():
    output = DeliveryBackend()
    effect = SendEffect(output)

    serialized = effect.serialize()

    assert serialized == (
        'send',
        [(
            'tests.tmp.DeliveryBackend',
            (),
            {}
        )],
        {}
    )
