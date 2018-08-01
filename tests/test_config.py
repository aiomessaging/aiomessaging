import pytest

from aiomessaging.config import Config
from aiomessaging.utils import class_from_string


def test_get_logging_dict():
    conf = Config()
    assert isinstance(conf.get_logging_dict(), dict)


def test_log_format():
    conf = Config({'log_format': '123'})
    assert conf.get_log_format() == '123'


def test_class_from_string():
    with pytest.raises(Exception):
        class_from_string('SomethingWrong', base='tests.tmp')

    class_from_string('DeliveryBackend', base='tests.tmp')
