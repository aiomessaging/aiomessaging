"""
aiomessaging can be started with `python -m aiomessaging`
"""
from aiomessaging import AiomessagingApp
# pylint: disable=invalid-name
app = AiomessagingApp('example.yml')  # pragma: no cover
app.start()  # pragma: no cover
