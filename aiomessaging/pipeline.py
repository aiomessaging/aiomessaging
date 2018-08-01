"""Pipelines.
"""
import asyncio
from typing import List


class EventPipeline:
    """Event pipeline.
    """
    def __init__(self, config: List) -> None:
        self.callable_list = config

    async def __call__(self, value):
        """Process regarding to pipeline configuration.
        """
        for action in self.callable_list:
            intermediate = action(value)
            if intermediate is not None:
                value = intermediate
        return value


class GenerationPipeline:
    """Generation pipeline.

    Executes in parallel.
    """
    def __init__(self, config: List) -> None:
        self.callable_list = config

    async def __call__(self, queue, event):
        childs = self.callable_list
        event.log.debug("Start generation pipeline")
        result = await asyncio.gather(
            *(item(event, queue) for item in childs)
        )
        event.log.debug("Generation pipeline finished with %s", result)
        return result
