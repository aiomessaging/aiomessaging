"""Messaging queues.
"""
from .backend import QueueBackend
from .queue import Queue, AbstractQueue

__all__ = ['QueueBackend', 'Queue', 'AbstractQueue']
