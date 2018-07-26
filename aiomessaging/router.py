"""Router.
"""
from .message import Message
from .effects import EffectStatus, send
from .utils import class_from_string


class Router:
    """Message router.

    Routes messages through output backends.

    Backend is chosen based on message route calculated at message pipeline
    stage and configuration.
    """

    def __init__(self, output_pipeline):
        self.output_pipeline = output_pipeline

    def next_output(self, message: Message):
        """Select output for message.
        """
        pipeline = self.get_pipeline(message)
        send_value = None
        try:
            while True:
                effect = pipeline.send(send_value)
                status = message.effect_status(effect)

                if not status == EffectStatus.PENDING and status:
                    continue

                state = message.effect_state(effect)

                while True:
                    action = effect.next_action(state)

                    if action is None:
                        message.set_effect_status(
                            effect, EffectStatus.FINISHED
                        )

                    output = action.get_output()
                    if output:
                        return output
                    action.execute()
        except StopIteration:
            # No more routes available (all finished or failed)
            return None

    def get_pipeline(self, message: Message):
        """Get delivery pipeline.
        """
        if isinstance(self.output_pipeline, str):
            # TODO: not a class :-)
            # string pointer to delivery pipeline generator
            pipeline_gen = class_from_string(self.output_pipeline)
            pipeline = pipeline_gen(message)
        elif callable(self.output_pipeline):
            pipeline = self.output_pipeline(message)
        elif isinstance(self.output_pipeline, list):
            pipeline = generator_from_backend_list(self.output_pipeline)
        else:
            raise TypeError(
                "Type `%s` can't be used for `output_pipeline_argument`"
                % type(self.output_pipeline)
            )
        return pipeline


def generator_from_backend_list(backends):
    """Simple generator from list of backends.
    """
    yield send(*backends)
