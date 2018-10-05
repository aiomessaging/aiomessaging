"""
aiomessaging can be started with `python -m aiomessaging`
"""
import click
from aiomessaging import AiomessagingApp


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
@click.argument('config')
def worker(config):
    """Start worker node.
    """
    try:
        app = AiomessagingApp(config)
        app.start()
    except click.Abort:
        click.echo("Shutdown")
        app.stop()


cli.add_command(init)
cli.add_command(worker)
