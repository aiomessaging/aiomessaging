"""
Output consumer test suite.
"""
import asyncio
import pytest

from aiomessaging.contrib.dummy.pipelines import (
    sequence_pipeline,
    failing_output_pipeline,
    all_dummy_pipeline,
)

# pylint:disable=unused-import
from .fixtures import backend  # noqa

from .helpers import (
    OutputConsumerContext,
    MessageConsumerContext,
    send_test_message,
    log_count,
)


@pytest.mark.asyncio
async def test_output_consumer_handler(backend, caplog):
    async with OutputConsumerContext(backend, 'null', sequence_pipeline) as consumer:
        await send_test_message(backend.connection, consumer.queue.name)
        await consumer.last_messages.get()

    assert log_count(caplog, level='ERROR') == 0


@pytest.mark.asyncio
async def test_failing_output(backend, caplog):
    async with OutputConsumerContext(backend, 'null', failing_output_pipeline) as consumer:
        await send_test_message(backend.connection, consumer.queue.name)
        await consumer.last_messages.get()

    assert log_count(caplog, level='ERROR') == 1


@pytest.mark.asyncio
async def test_dummy_consumers(backend, caplog):
    async with OutputConsumerContext(backend, 'console', all_dummy_pipeline):
        async with OutputConsumerContext(backend, 'check', all_dummy_pipeline):
            async with OutputConsumerContext(backend, 'retry', all_dummy_pipeline):
                    async with OutputConsumerContext(backend, 'never', all_dummy_pipeline) as last_ouput:
                        async with MessageConsumerContext(backend, all_dummy_pipeline) as consumer:
                            await send_test_message(backend.connection, consumer.queue.name)
                            await last_ouput.last_messages.get()

    assert log_count(caplog, level='ERROR') == 0
