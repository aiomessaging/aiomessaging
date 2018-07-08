from .message import Message


class Router(object):
    """Message router.

    Routes messages through output backends.

    Backend is chosen based on message route calculated at message pipeline
    stage.
    """

    def __init__(self):
        pass

    def next_hope(self, message: Message):
        pass

    def select_output(self, event_type, message: Message):
        return "sns"
