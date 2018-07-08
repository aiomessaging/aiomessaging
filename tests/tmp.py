"""FIXME: where it belong to?
"""
import logging

from aiomessaging.event import Event


log = logging.getLogger('aiomessaging')


class ExampleFilter(object):
    def __init__(self, *args, **kwargs):
        pass

    def __call__(self, event):
        pass


class OneMessage(object):
    def __init__(self, *args, **kwargs):
        pass

    async def __call__(self, event: Event, tmp_queue):
        log.info("add message to queue %s, %s", tmp_queue.routing_key, event)
        await tmp_queue.publish(
            body={"event": event},
            routing_key=tmp_queue.routing_key
        )
        # for i in range(1):
        #     await tmp_queue.put(Message(event=event, content={'a': i}))


class DeliveryBackend(object):
    def __init__(self, *args, **kwargs):
        pass

    def __call__(self, message):
        pass
