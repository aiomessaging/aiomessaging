"""
contrib.dummy module

Contains no-op messaging components for testing proposes.
"""
from .output import (
    NullOutput,
    ConsoleOutput,
    FailingOutput,
    CheckOutput,
    NeverDeliveredOutput,
    RetryOutput,
)
from .generators import DummyGenerator
from .filters import NoopFilter


__all__ = [
    'NullOutput',
    'ConsoleOutput',
    'FailingOutput',
    'CheckOutput',
    'NeverDeliveredOutput',
    'RetryOutput',

    'DummyGenerator',

    'NoopFilter',
]
