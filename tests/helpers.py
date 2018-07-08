import logging

from aiomessaging.queues import QueueBackend

logging.getLogger("pika").setLevel(logging.INFO)
logging.getLogger("aio_pika").setLevel(logging.INFO)


async def send_test_message(connection, queue_name="aiomessaging.tests",
                            body=None):
    backend = QueueBackend()
    await backend.connect()
    await backend.publish(exchange='', routing_key=queue_name, body=body)


def has_log_message(caplog, message=None, level=None):
    """Check caplog contains log message.
    """
    for r in caplog.records:
        if level and r.levelname != level:
            continue
        if not message or message in r.getMessage() or message in r.exc_text:
            return True
    return False


def log_count(caplog, message=None, level=None):
    result = 0
    for r in caplog.records:
        if level and r.levelname == level:
            result += 1
            continue
        if message and message in r.getMessage():
            result += 1
    return result
