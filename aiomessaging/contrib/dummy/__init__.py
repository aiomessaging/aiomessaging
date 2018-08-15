"""
contrib.dummy module

Contains no-op outputs and generators for testing proposes.
"""
from .output import (
    NullOutput,
    ConsoleOutput,
    FailingOutput,
    CheckOutput,
)

__all__ = [
    'NullOutput',
    'ConsoleOutput',
    'FailingOutput',
    'CheckOutput',
]
