"""
aiomessaging can be started with `python -m aiomessaging`
"""
import json
import asyncio

import click

from . import AiomessagingApp


@click.group()
def cli():
    """aiomessaging cli.
    """
    pass


@click.command()
def init():
    """Init messaging install.
    """
    click.echo("No-op for a while")


@click.command()
@click.option('-c', '--config')
def worker(config):
    """Start worker node.
    """
    try:
        app = AiomessagingApp(config)
        app.start()
    except click.Abort:
        click.echo("Shutdown")
        app.stop()


@click.command()
@click.argument('event_type')
@click.argument('payload', required=False)
@click.option('-c', '--config')
@click.option('--count', default=1)
@click.option('--loop/--no-loop', default=False)
def send(event_type, payload, *args, **kwargs):
    """Create and send event.
    """
    if payload is None:
        payload = {
            'a': 1
        }
    else:
        payload = json.loads(payload)

    asyncio.run(send_event(event_type, payload, *args, **kwargs))

    click.echo("Events was published")


async def send_event(event_type, payload, config, count, loop):
    """Send event to input queue.
    """
    app = AiomessagingApp(config)

    while True:
        try:
            for _ in range(count):
                await app.send(event_type, payload)
            await asyncio.sleep(1)
        except KeyboardInterrupt:
            break
        if not loop:
            break

cli.add_command(init)
cli.add_command(worker)
cli.add_command(send)
