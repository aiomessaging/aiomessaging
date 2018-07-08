"""Pipelines.
"""
import logging
import asyncio
from typing import List

from .logging import LoggerAdapter


class Pipeline:
    """Base pipeline class.
    """
    def __init__(self, config: List) -> None:
        self.callable_list = config
        self._log = logging.getLogger(__name__)
        self.log = LoggerAdapter(self._log, {})

    async def process(self, value):
        """Process regarding to pipeline configuration.
        """
        for action in self.callable_list:
            intermediate = action(value)
            if intermediate is not None:
                value = intermediate
        return value

    async def __call__(self, value):
        return await self.process(value)


# pylint: disable=too-few-public-methods
class ParallelPipeline(Pipeline):
    """Parallel pipeline.

    Executes steps in parallel (asynio).
    """
    async def process(self, value):
        """Process value in parallel.

        TODO: copy value object for each item and check conflicts
        """
        result = {}
        data = await asyncio.gather(
            *(item(value) for item in self.callable_list)
        )
        for item in data:
            if item is not None:
                result.update(item)
        return result


class EventPipeline(Pipeline):
    """Event pipeline.
    """
    pass


class GenerationPipeline(ParallelPipeline):
    """Generation pipeline.

    Executes in parallel.
    """
    async def __call__(self, queue, event):
        childs = self.callable_list
        self.log.debug("Start generation pipeline")
        result = await asyncio.gather(
            *(item(event, queue) for item in childs)
        )
        self.log.debug("Generation pipeline finished with %s", result)
        return result


class MessagePipeline(Pipeline):
    """Message pipeline.
    """
    pass


class DeliveryPipeline(ParallelPipeline):
    """Delivery pipeline.
    """
    pass
