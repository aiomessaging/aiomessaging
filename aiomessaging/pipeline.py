"""Pipelines.
"""
import asyncio
from typing import List


class Pipeline:
    """Base pipeline class.
    """
    def __init__(self, config: List) -> None:
        self.callable_list = config

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


class ParallelPipeline(Pipeline):
    """Parallel pipeline.

    Executes steps in parallel (asyncio).
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
        event.log.debug("Start generation pipeline")
        result = await asyncio.gather(
            *(item(event, queue) for item in childs)
        )
        event.log.debug("Generation pipeline finished with %s", result)
        return result
