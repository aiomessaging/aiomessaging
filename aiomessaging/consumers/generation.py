"""Generation consumer.
"""
import time
import asyncio
from typing import Dict, Optional

from ..message import Message
from ..queues import AbstractQueue

from .base import MessageConsumerMixIn, BaseConsumer


QUEUE_FORGET_TIMEOUT = 1


class GenerationConsumer(MessageConsumerMixIn, BaseConsumer):

    """Generation consumer.

    Recive message from tmp generation queue and place them to the provided
    messages queue.
    """

    # messages from tmp generation queue will be drained to this queue
    messages_queue: AbstractQueue

    # last received message time for each consumed queue
    last_recived_time: Dict[AbstractQueue, int]

    _consumer_monitoring_task: Optional[asyncio.Task]

    def __init__(self, messages_queue: AbstractQueue, **kwargs) -> None:
        super().__init__(**kwargs)
        self.messages_queue = messages_queue
        self.last_recived_time = {}
        self.last_time = time.time()

        self._consumer_monitoring_task = None

    async def start(self):
        """Start generation consumer.

        Also starts monitoring task.
        """
        await super().start()
        self._start_consumer_monitoring()

    async def stop(self):
        """Stop generation consumer.

        Also stop generation task.
        """
        await super().stop()
        await self._stop_consumer_monitoring()

    async def handle_message(self, message: Message):
        message.log.info("Send message to output")
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

    def consume(self, queue):
        """Start consume provided queue.
        """
        super().consume(queue)
        self.last_recived_time[queue] = time.time()

    def cancel(self, queue):
        """Stop consume provided queue.
        """
        super().cancel(queue)
        del self.last_recived_time[queue]

    # pylint: disable=arguments-differ
    def _handler(self, queue, *args, **kwargs):
        """Generation queue handler.

        Catch queue argument and update last message time for this queue.
        """
        self.last_recived_time[queue] = time.time()
        super()._handler(queue, *args, **kwargs)

    def _start_consumer_monitoring(self):
        """Start monitoring task.
        """
        self._consumer_monitoring_task = self.loop.create_task(
            self._consumer_monitoring()
        )

    async def _stop_consumer_monitoring(self):
        """Cancel monitoring task.
        """
        if self._consumer_monitoring_task:
            await self._consumer_monitoring_task
        else:
            self.log.info('No logs at all')

    async def _consumer_monitoring(self):
        """Consumer monitoring coroutine.
        """
        while self.running:
            for queue, last_time in self.last_recived_time.copy().items():
                if time.time() - last_time > QUEUE_FORGET_TIMEOUT:
                    self.cancel(queue)
                    queue.log.debug('Empty. Cancel by generation monitoring after %f',
                                    QUEUE_FORGET_TIMEOUT)
            await asyncio.sleep(1)
