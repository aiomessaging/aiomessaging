"""Exceptions.
"""


class DropException(Exception):
    """Drop event or message exception.

    Consumer must drop message when handler raise this exception.
    """
    pass


class DelayException(Exception):
    """Event or Message must be delayed exception.

    Usable for things like event/message cancellation (for ex: liked comment
    was disliked in two seconds and it is not neccesary to delivery both
    messages to user)
    """

    # def __init__(self, delay):
    #     self.delay = delay
