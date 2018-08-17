"""
pipelines for tests based on dummy components
"""
from aiomessaging.effects import send
from aiomessaging.contrib.dummy import (
    NullOutput,
    FailingOutput,
    NeverDeliveredOutput,
    ConsoleOutput,
    CheckOutput,
    RetryOutput,
)


def simple_pipeline(message):
    """Simple pipeline.

    Send message through test delivery backend
    """
    yield send(NullOutput(), NullOutput())


def sequence_pipeline(message):
    """Sequence pipeline.

    Send to test backend twice.
    """
    yield send(NullOutput(test_arg=2))
    yield send(NullOutput(test_arg=1))


def failing_output_pipeline(message):
    yield send(FailingOutput())


def all_dummy_pipeline(message):
    available_outputs = (
        ConsoleOutput,
        CheckOutput,
        RetryOutput,
        NeverDeliveredOutput,
    )

    for dummy_output in available_outputs:
        yield send(dummy_output())


def example_pipeline(message):
    yield send(RetryOutput(), NeverDeliveredOutput())
    yield send(ConsoleOutput())
