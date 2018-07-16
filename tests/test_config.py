from aiomessaging.config import Config


def test_get_logging_dict():
    conf = Config()
    assert isinstance(conf.get_logging_dict(), dict)


def test_log_format():
    conf = Config({'log_format': '123'})
    assert conf.get_log_format() == '123'
