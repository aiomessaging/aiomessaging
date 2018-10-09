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
@click.argument('payload')
@click.option('-c', '--config')
@click.option('--count', default=1)
def send(event_type, payload, config, count):
    """Create and send event.
    """
    app = AiomessagingApp(config)

    if payload is None:
        payload = {
            'a': 1
        }
    else:
        payload = json.loads(payload)

    for _ in range(count):
        asyncio.run(app.send(event_type, payload))

    click.echo("Events was published")


cli.add_command(init)
cli.add_command(worker)
cli.add_command(send)
