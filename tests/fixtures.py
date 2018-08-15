import pytest

from aiomessaging import QueueBackend


@pytest.fixture
async def backend():
    backend = QueueBackend()
    await backend.connect()
    yield backend
    await backend.close()
