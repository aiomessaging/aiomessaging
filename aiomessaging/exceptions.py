"""Exceptions.
"""


class MessagingException(Exception):

    """Base messaging exception.
    """

    pass


class FlowException(MessagingException):

    """Base flow exception.

    Flow exceptions are used to communicate with consumers.
    """

    pass


class Retry(FlowException):

    """Retry exception.

    Expected to be raised by output backend in case if message can't be
    delivered at the moment. May contain optional delay in case the backend
    know next try time and the reason to help in debug.

    :param delay: Amount of time to wait before next try.
    :param reason: Human-readable definition of retry reason for logs.
    """

    def __init__(self, reason, delay=None):
        super().__init__(reason)
        self.delay = delay
        self.reason = reason


class CheckDelivery(FlowException):

    """Delivery check required exception.

    Raised by output backend to notify consumer that message delivery must be
    checked at the next delivery cycle.

    Output can provide amount of time to delay if it know it exactly.

    :param delay: amount of time to wait before next check
    """

    def __init__(self, delay=None):
        super().__init__()
        self.delay = delay


# class DropException(Exception):
#     """Drop event or message exception.

#     Consumer must drop message when handler raise this exception.
#     """
#     pass


# class DelayException(Exception):
#     """Event or Message must be delayed exception.

#     Usable for things like event/message cancellation (for ex: liked comment
#     was disliked in two seconds and it is not neccesary to delivery both
#     messages to user)
#     """

#     def __init__(self, delay):
#         self.delay = delay
