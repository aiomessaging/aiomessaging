"""
aiomessaging can be started with `python -m aiomessaging`
"""
from aiomessaging import AiomessagingApp  # pragma: no cover
# pylint: disable=invalid-name
app = AiomessagingApp('example.yml')  # pragma: no cover
app.start()  # pragma: no cover
