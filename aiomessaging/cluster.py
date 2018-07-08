"""Cluster utils.
"""
import asyncio


from .consumers.base import BaseConsumer


class Cluster(BaseConsumer):
    """aiomessaging Cluster.

    Provide interface to other cluster instances.

    `generation_queue` used to communicate with application: every time
    Cluster receives `consume` message it place generation queue name to it in
    this queue. Application is responsible to create consumers for this queues.
    """

    generation_queue: asyncio.Queue

    def __init__(self, queue, exchange, loop, **kwargs):
        self.exchange = exchange

        self.generation_queue = asyncio.Queue(loop=loop)
        self.actions = {
            'consume': self.generation_queue
        }

        super().__init__(queue=queue, loop=loop, **kwargs)

    async def handler(self, message):
        """Handle cluster message
        """
        self.log.info("Cluster message recieved %s", message)

        self.log.debug("Message body:\n%s", message)
        body = message

        try:
            cluster_action = body['action']
        except KeyError:
            self.log.error("No action in message: %s", body)
            return

        try:
            queue = self.actions[cluster_action]
        except KeyError:
            self.log.error("Invalid action")
            return

        try:
            await queue.put(body['queue_name'])
        except KeyError:
            self.log.error("No queue name in message %s", body)
            return

    async def start_consume(self, queue_name):
        """Publish message to cluster to start consume queue with generated messages.
        """
        await self.exchange.publish(
            {'action': 'consume', 'queue_name': queue_name}
        )
        self.log.debug('cluster queue name %s', self.queue.name)
        self.log.info("tell cluster to start consume %s", queue_name)
