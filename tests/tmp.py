"""
TODO: need better place for this stuff
"""
import logging

from aiomessaging.event import Event
from aiomessaging.message import Message
from aiomessaging.outputs import AbstractOutputBackend
from aiomessaging.effects import send


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


class DeliveryBackend2(AbstractOutputBackend):
    name = 'sns2'

    def __call__(self, message):
        pass

    def check(self, message):
        pass

    def send(self, message):
        pass


def simple_pipeline(message):
    """Simple pipeline.

    Send message through test delivery backend
    """
    yield send(DeliveryBackend())


def sequence_pipeline(message):
    """Sequence pipeline.

    Send to test backend twice.
    """
    yield send(DeliveryBackend(test_arg=2))
    yield send(DeliveryBackend2(test_arg=1))
