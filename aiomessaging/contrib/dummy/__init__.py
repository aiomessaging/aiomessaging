"""
contrib.dummy module

Contains no-op outputs and generators for testing proposes.
"""
from .output import (
    NullOutput,
    ConsoleOutput,
    FailingOutput,
    CheckOutput,
    NeverDeliveredOutput,
    RetryOutput,
)

__all__ = [
    'NullOutput',
    'ConsoleOutput',
    'FailingOutput',
    'CheckOutput',
    'NeverDeliveredOutput',
    'RetryOutput',
]
