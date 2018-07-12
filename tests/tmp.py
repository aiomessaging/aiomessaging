"""FIXME: where it belong to?
"""
import logging

from aiomessaging.event import Event
from aiomessaging.message import Message


log = logging.getLogger('aiomessaging')


class ExampleFilter:
    def __init__(self, *args, **kwargs):
        pass

    def __call__(self, event):
        pass


class OneMessage:
    def __init__(self, *args, **kwargs):
        pass

    async def __call__(self, event: Event, tmp_queue):
        message = Message(event=event, content={'a': 'something'})
        await tmp_queue.publish(
            body=message.to_dict(),
            routing_key=tmp_queue.routing_key
        )
        event.log.debug("Published to tmp queue")
        # for i in range(1):
        #     await tmp_queue.put(Message(event=event, content={'a': i}))


class DeliveryBackend:
    def __init__(self, *args, **kwargs):
        pass

    def __call__(self, message):
        pass
