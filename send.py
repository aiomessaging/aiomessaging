#!/usr/bin/env python
import argparse
import asyncio

from aiomessaging import QueueBackend


parser = argparse.ArgumentParser(
    description="Send message to the events queue of aiomessaging"
)
parser.add_argument('count', nargs='?', type=int, default=1)


async def main(loop):
    args = parser.parse_args()

    backend = QueueBackend(loop=loop)
    await backend.connect()

    routing_key = "events.example_event"

    for i in range(args.count):
        await backend.publish(
            exchange='',
            routing_key=routing_key,
            body={
                "type": "example_event", "event": "example_event", "a": i
            }
        )

    await backend.close()
    await asyncio.sleep(1)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.stop()
