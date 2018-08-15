"""
Configuration tests.
"""
import pytest

from aiomessaging.config import Config
from aiomessaging.utils import class_from_string


def test_get_logging_dict():
    """Test getting logging dict from config.

    Just for coverage: we don't use logging when measuring coverage.
    """
    conf = Config()
    assert isinstance(conf.get_logging_dict(), dict)


def test_log_format():
    """Test log format configuration.

    Just for coverage: we don't use logging when measuring coverage.
    """
    conf = Config({'log_format': '123'})
    assert conf.get_log_format() == '123'


def test_class_from_string():
    """Test class_from_string function.
    """
    with pytest.raises(Exception):
        class_from_string(
            'SomethingWrong',
            base='aiomessaging.contrib.dummy'
        )

    class_from_string(
        'NullOutput', base='aiomessaging.contrib.dummy'
    )
