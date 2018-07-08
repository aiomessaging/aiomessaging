"""Router.
"""
from .message import Message


class Router:
    """Message router.

    Routes messages through output backends.

    Backend is chosen based on message route calculated at message pipeline
    stage.
    """

    def __init__(self):
        pass

    # pylint: disable=unused-argument
    def next_hope(self, message: Message):
        """Next hope for the message.
        """
        pass

    # pylint: disable=unused-argument,no-self-use
    def select_output(self, event_type, message: Message):
        """Select output for message.
        """
        return "sns"
