"""
TODO: need better place for this stuff
"""
import logging

from aiomessaging.event import Event
from aiomessaging.message import Message
from aiomessaging.outputs import AbstractOutputBackend


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
        message = Message(event_type=event.type, event_id=event.id, content={'a': 'something'})
        await tmp_queue.publish(
            body=message.to_dict(),
            routing_key=tmp_queue.routing_key
        )
        event.log.debug("Published to tmp queue")
        # for i in range(1):
        #     await tmp_queue.put(Message(event=event, content={'a': i}))


class DeliveryBackend(AbstractOutputBackend):
    name = 'sns'

    def __call__(self, message):
        pass

    def check(self, message):
        pass

    def send(self, message):
        pass
