"""Router.
"""
from .message import Message
from .effects import EffectStatus, send
from .utils import class_from_string


class Router:
    """Message router.

    Routes messages through output backends.
    """

    def __init__(self, output_pipeline):
        self.output_pipeline = output_pipeline

    def next_effect(self, message: Message):
        """Select next effect for message.

        Return `None` if no more routes available.
        """
        pipeline = self.get_pipeline(message)
        send_value = None
        try:
            while True:
                effect = pipeline.send(send_value)
                status = message.get_route_status(effect)
                if not status == EffectStatus.PENDING:
                    continue
                return effect
        except StopIteration:
            # No more routes available (all finished or failed)
            return None

    def apply_next_effect(self, message):
        """Apply next effect for message.
        """
        effect = self.next_effect(message)
        new_state = effect.apply(message)
        message.set_route_state(effect, new_state)
        if effect.next_action(message.get_route_state(effect)):
            message.set_route_status(effect, EffectStatus.PENDING)
        else:
            message.set_route_status(effect, EffectStatus.FINISHED)

    def skip_next_effect(self, message):
        """Skip next effect action.
        """
        effect = self.next_effect(message)
        new_state = effect.skip_next(state=message.get_route_state(effect))
        message.set_route_state(effect, new_state)
        if effect.next_action(message.get_route_state(effect)):
            message.set_route_status(effect, EffectStatus.PENDING)
        else:
            message.set_route_status(effect, EffectStatus.FINISHED)

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
        else:  # pragma: no cover
            raise TypeError(
                "Type `%s` can't be used for `output_pipeline_argument`"
                % type(self.output_pipeline)
            )
        return pipeline


def generator_from_backend_list(backends):
    """Simple generator from list of backends.
    """
    backend_instances = []
    for backend in backends:
        backend_cls = class_from_string(backend)
        backend_instances.append(backend_cls())
    yield send(*backend_instances)
