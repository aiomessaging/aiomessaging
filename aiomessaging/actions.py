"""
message consumer actions
"""
import abc


class Action(abc.ABC):

    """Message consumer action.

    Action can provide next output backend or execute something on routing
    phase.

    Either `get_output` or `execute` must be implemented on derived class.

    TODO: fix bad interface. It is possible to raise something like
          NextOutputException
    """

    def get_output(self):
        """Get next output backend.

        Return output backend or `None`.
        """
        pass  # pragma: no cover

    def execute(self, message):
        """Execute action.
        """
        pass  # pragma: no cover


class SendOutputAction(Action):

    """Action: send message through output.
    """

    def __init__(self, output):
        self.output = output

    def get_output(self):
        return self.output

    def execute(self, message):
        return self.output.send(message)


# class CheckOutputAction(Action):
#
#     """Action: check message delivery on output.
#
#     Adds check task to output queue instead of direct `check` call.
#     """
#
#     def __init__(self, output):
#         self.output = output
#
#     def get_output(self):
#         return self.output


# class CallAction(Action):

#     """Action: call function.

#     Can be used to call function to perform heavy calculations outside of
#     pipeline. `func` will be executed in `MessageConsumer` handler.
#     """

#     def __init__(self, func, *args, **kwargs):
#         self.callable = func
#         self.call_args = args
#         self.call_kwargs = kwargs

#     def execute(self):
#         return self.callable(*self.call_args, **self.call_kwargs)
