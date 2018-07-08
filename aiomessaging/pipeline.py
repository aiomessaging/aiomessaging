import logging
import asyncio
from typing import List

from .logging import LoggerAdapter


class Pipeline(object):
    def __init__(self, config: List):
        self.callable_list = config
        self._log = logging.getLogger(__name__)
        self.log = LoggerAdapter(self._log, {})

    async def process(self, value):
        for c in self.callable_list:
            intermediate = c(value)
            if intermediate is not None:
                value = intermediate
        return value

    async def __call__(self, value):
        return await self.process(value)


class ParallelPipeline(Pipeline):
    async def process(self, event):
        # FIXME: copy event object for each item and check conflicts
        result = {}
        data = await asyncio.gather(
            *(item(event) for item in self.callable_list)
        )
        for item in data:
            if item is not None:
                result.update(item)
        return result


class EventPipeline(Pipeline):
    pass


class GenerationPipeline(ParallelPipeline):
    async def __call__(self, queue, event):
        childs = self.callable_list
        # import ipdb; ipdb.set_trace()
        self.log.debug("Start generation pipeline")
        result = await asyncio.gather(
            *(item(event, queue) for item in childs)
        )
        self.log.debug("Result was %s", result)
        return result


class MessagePipeline(Pipeline):
    pass


class DeliveryPipeline(ParallelPipeline):
    pass
