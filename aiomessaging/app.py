"""Messaging application.
"""
import os
import asyncio
import logging
from logging.config import dictConfig

from .config import Config
from .consumers import ConsumersManager
from .queues import QueueBackend


def apply_logging_configuration(config):  # pragma: no cover
    """Apply dict logging configuration.

    Allows to mock logging in test to prevent caplog overwrites.
    """
    dictConfig(config)


# pylint: disable=too-many-instance-attributes
class AiomessagingApp:
    """aiomessaging application.
    """
    queue: QueueBackend
    config: Config

    consumers: ConsumersManager

    log: logging.Logger

    def __init__(self, config=None, loop=None):
        self.loop = loop

        self.set_event_loop(loop)

        self.config = Config()

        if config:
            self.config.from_file(config)

        self.configure_logging()
        self.log.info('Configuration file: %s', config)

        self.queue = self.config.get_queue_backend()

        self.consumers = ConsumersManager(self.config, self.queue)

    def start(self, loop=None):
        """Start aiomessaging application.
        """
        self.log.info("aiomessaging service was started. PID: %i",
                      os.getpid())
        assert self.config is not None, "Config not provided"

        self.loop = loop or self.loop or asyncio.get_event_loop()
        self.loop.set_debug(True)

        self.loop.run_until_complete(self._start())

        try:
            self.loop.run_forever()
        except KeyboardInterrupt:  # pragma: no cover
            print(" — Ctrl + C was pressed")
            self.log.info("Graceful shutdown. Press Ctrl + C to exit")
        finally:
            try:
                self.loop.run_until_complete(self.shutdown())
                self.loop.run_until_complete(self.loop.shutdown_asyncgens())
                self.loop.close()
                self.log.info("Loop closed.")
            except KeyboardInterrupt:  # pragma: no cover
                print(" — Ctrl + C was pressed second time")
                self.log.error("Stopped hard. Exiting.")
                exit(1)

    async def _start(self):
        """Start all application coroutines.
        """
        await self.queue.connect()

        await self.consumers.start_all(loop=self.loop)

    async def send(self, event_type, payload=None):
        """Publish event to the events queue.
        """
        if not self.queue.is_open:
            await self.queue.connect()  # pragma: no cover

        queue = await self.queue.events_queue(event_type)
        # because we publish to '' exchange by default
        routing_key = "events.%s" % event_type
        await queue.publish(payload, routing_key=routing_key)

    def configure_logging(self):
        """Configure logging.
        """
        self.log = logging.getLogger(__name__)
        apply_logging_configuration(self.config.get_logging_dict())

    def set_event_loop(self, loop):
        """Set event loop to run on.
        """
        self.loop = loop or asyncio.get_event_loop()
        self.loop.set_debug(True)

    def stop(self):
        """Stop application event loop.
        """
        self.log.debug('Stopping event loop')
        self.loop.stop()

    async def shutdown(self):
        """Shutdown application gracefully.
        """
        await self.consumers.stop_all()

        await self.queue.close()
        self.log.info("Shutdown complete.")
