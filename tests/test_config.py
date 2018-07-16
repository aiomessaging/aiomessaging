from aiomessaging.config import Config


def test_get_logging_dict():
    conf = Config()
    assert isinstance(conf.get_logging_dict(), dict)
