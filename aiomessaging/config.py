# pylint: disable=missing-docstring
"""aiomessaging config.
"""
from typing import Dict

import yaml

from .queues import QueueBackend
from .pipeline import EventPipeline, GenerationPipeline
from .utils import class_from_string


# pylint: disable=too-many-ancestors
class ConfigLoader(yaml.Loader):
    """YAML config loader.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_constructor('!class', ConfigLoader.create_class)
        self.add_path_resolver(
            '!class',
            ['events', None, 'event_pipeline', None]
        )
        self.add_path_resolver(
            '!class',
            ['events', None, 'generators', None]
        )
        self.add_path_resolver(
            '!class',
            ['delivery', None, 'outputs', None]
        )

    @staticmethod
    def create_class(loader, node):
        """Create class instance from yaml node.
        """
        kwargs = {}
        if isinstance(node, yaml.MappingNode):
            kwargs = loader.construct_mapping(node)
            class_name = [v for v in kwargs.keys() if kwargs[v] is None][0]
        else:
            class_name = loader.construct_scalar(node)
        try:
            # pylint: disable=invalid-name
            ObjClass = class_from_string(class_name)
        except Exception:  # pragma: no cover
            raise yaml.MarkedYAMLError(f"`{class_name}` not found",
                                       node.start_mark)
        obj = ObjClass(**kwargs)
        return obj


class BaseConfig(dict):
    """Base messaging config.
    """
    def from_file(self, filename: str):
        """Load config from file.
        """
        with open(filename, 'r') as fp:
            self.from_fp(fp)

    def from_fp(self, fp):
        """Load config from file pointer.
        """
        self.from_string(fp.read())

    def from_string(self, data: str):
        """Load config from string.
        """
        config = yaml.load(data, ConfigLoader)
        self.from_dict(config)

    def from_dict(self, config: Dict):
        """Load config from dict.

        All instances must be instantiated (will not pass through
        `ConfigLoader`)
        """
        self.update(config)


class Config(BaseConfig):

    """aiomessaging config.

    Allow to abstract from config structure.
    """

    @property
    def events(self):
        return self.get('events', {})

    # pylint: disable=no-self-use
    def get_logging_dict(self):
        """Logging dict config.
        """
        return {
            "version": 1,
            "disable_existing_loggers": True,
            "formatters": {
                "default": {
                    "class": "logging.Formatter",
                    "format": "%(asctime)-15s %(levelname)-7s %(message)s"
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "default"
                }
            },
            "loggers": {
                "aiomessaging": {
                    "level": "INFO",
                    "handlers": ["console"],
                    "propagate": False
                }
            },
            "root": {
                "level": "ERROR",
                "handlers": ["console"]
            },
        }

    def get_event_pipeline(self, event_type):
        """Event pipeline for event.
        """
        event_config = self.get_event_config(event_type)
        pipeline = event_config.get('event_pipeline')
        if not isinstance(pipeline, EventPipeline):
            pipeline = EventPipeline(pipeline)
        return pipeline

    def get_generators(self, event_type):
        """Generation pipeline for event type.
        """
        event_config = self.get_event_config(event_type)
        pipeline = event_config.get('generators')
        if not isinstance(pipeline, GenerationPipeline):
            pipeline = GenerationPipeline(pipeline)
        return pipeline

    def get_event_config(self, event_type):
        """Config for particular event type.
        """
        return self.events[event_type]

    def get_queue_backend(self):
        """Queue backend instance.

        Instantiate queue backend based on configuration.
        """
        conf = self.get('queue', {'backend': 'rabbitmq'})
        if not conf:  # pragma: no cover
            raise Exception("No queue configuration")
        backend_name = conf.pop('backend')
        assert backend_name == 'rabbitmq', "No other choice for a while"
        return QueueBackend(**conf)
