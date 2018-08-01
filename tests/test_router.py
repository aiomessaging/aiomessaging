"""
router test suite
"""
from aiomessaging.router import Router
from aiomessaging.message import Message, Route
from aiomessaging.effects import SendEffect, EffectStatus

from .tmp import simple_pipeline, sequence_pipeline, DeliveryBackend


def test_simple_pipeline():
    """Test router constructor and simple pipeline
    """
    router = Router(output_pipeline=simple_pipeline)
    message = Message(event_id='test_simple', event_type='example_event')
    effect = router.next_effect(message)

    assert isinstance(effect, SendEffect)

    router.apply_next_effect(message)

    assert message.route
    assert isinstance(message.route[0], Route)
    assert message.route[0].effect == effect


def test_sequence_send():
    router = Router(output_pipeline=sequence_pipeline)
    message = Message(event_id='test_sequence', event_type='example_event')

    effect = router.next_effect(message)
    assert isinstance(effect, SendEffect)
    assert effect.next_action().get_output().kwargs == {'test_arg': 2}

    router.apply_next_effect(message)

    assert message.route
    assert isinstance(message.route[0], Route)
    assert message.route[0].status == EffectStatus.FINISHED
    effect = router.next_effect(message)
    assert isinstance(effect, SendEffect)
    assert effect.next_action().get_output().kwargs == {'test_arg': 1}


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
