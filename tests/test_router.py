"""
router test suite
"""
from aiomessaging.router import Router
from aiomessaging.message import Message, Route
from aiomessaging.effects import SendEffect, EffectStatus
from aiomessaging.actions import SendOutputAction
from aiomessaging.contrib.dummy import NullOutput

from .tmp import simple_pipeline, sequence_pipeline


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
    """Test sequence pipeline flow.
    """
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

    router.apply_next_effect(message)

    assert router.next_effect(message) is None


def test_string_configuration():
    """Test string output pipeline configuration allowed.
    """
    message = Message(event_id='test_sequence', event_type='example_event')
    router = Router(output_pipeline='tests.tmp.simple_pipeline')
    router.get_pipeline(message)


def test_send_effect():
    """
    TODO: move to test_effects
    """
    output = NullOutput()
    effect = SendEffect(output)

    serialized = effect.serialize()

    assert serialized == (
        'send',
        [(
            'aiomessaging.contrib.dummy.output.NullOutput',
            (),
            {}
        )],
        {}
    )


def test_string_list_init():
    """Test pipeline initialization from string (class) list.
    """
    message = Message(event_id='test_sequence', event_type='example_event')
    router = Router(['tests.tmp.NullOutput'])
    pipeline = router.get_pipeline(message)
    effect = pipeline.send(None)
    action = effect.next_action()
    assert isinstance(action, SendOutputAction)
    assert isinstance(action.output, NullOutput)
