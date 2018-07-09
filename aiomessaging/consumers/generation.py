"""Generation consumer.
"""
import time
import asyncio

from ..message import Message

from .base import MessageConsumerMixIn, SingleQueueConsumer


# FIXME: consume multiple
class GenerationConsumer(MessageConsumerMixIn, SingleQueueConsumer):

    """Generation consumer.

    Recive message from tmp generation queue and places them to the output
    queue `messages.<type>`. Created by messaging app on cluster signal.

    Produce additional `_monitor_generation` coro which looks for queue end and
    periodically exams last handling time to recognize this.
    """

    monitor_gen_task = None

    def __init__(self, messages_queue, **kwargs):
        super().__init__(**kwargs)
        self.messages_queue = messages_queue
        self.last_time = time.time()

    async def start(self):
        await super().start()
        self.monitor_gen_task = self.loop.create_task(
            self._monitor_generation()
        )

    async def stop(self):
        self.log.info("Stopping")

        # pylint: disable=protected-access
        if self.messages_queue._backend.is_open:
            self.messages_queue.close()

        if self.monitor_gen_task:
            self.monitor_gen_task.cancel()
            try:
                exc = None
                await asyncio.wait_for(self.monitor_gen_task, 5)
                exc = self.monitor_gen_task.exception()
                if exc:
                    raise exc
            except asyncio.InvalidStateError:
                pass
            except AttributeError:
                pass
            except asyncio.CancelledError:
                pass
            if exc:
                self.log.error(
                    'Exception found in generation monitor task %s', type(exc)
                )
        await super().stop()

    async def handle_message(self, message: Message):
        self.last_time = time.time()
        self.log.debug("Generated message recieved %s", message)
        await self.send_output(message)

    async def send_output(self, message: Message):
        """Send message to messages queue.
        """
        await self.messages_queue.publish(
            message.to_dict(),
            routing_key=message.type
        )
        self.log.debug("Generated message passed to output exchange %s",
                       self.messages_queue)

    async def _monitor_generation(self):
        while self.running:
            time_diff = time.time() - self.last_time
            if time_diff > 1:
                self.log.debug(
                    "Queue %s seems empty. Stop generation consumer",
                    self.messages_queue.name
                )
                await self.stop()
                break
            await asyncio.sleep(0.1)
