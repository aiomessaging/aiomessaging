"""
dummy generators

For testing proposes.
"""
from aiomessaging import Event, Message


class DummyGenerator:

    """Dummy generator.

    Generate provided number of messages from event.

    :param int msg_count: Number of messages to generate from event.
    """

    def __init__(self, msg_count=1):
        self.msg_count = msg_count

    async def __call__(self, event: Event, tmp_queue):
        for i in range(self.msg_count):
            message = Message(event_type=event.type, event_id=event.id,
                              content={'a': i})
            await tmp_queue.publish(
                body=message.to_dict(),
                routing_key=tmp_queue.routing_key
            )
