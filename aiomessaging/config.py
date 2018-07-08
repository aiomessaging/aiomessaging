import yaml

from typing import Dict
from attrdict import AttrDict

from .queues import QueueBackend
from .pipeline import EventPipeline, GenerationPipeline
from .utils import class_from_string


class ConfigLoader(yaml.Loader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_constructor('!class', self.create_class)
        self.add_path_resolver(
            '!class',
            ['events', None, 'event_pipeline', None]
        )
        self.add_path_resolver(
            '!class',
            ['events', None, 'generators', None]
        )

    def create_class(self, loader, node):
        kwargs = {}
        if isinstance(node, yaml.MappingNode):
            kwargs = loader.construct_mapping(node)
            class_name = [v for v in kwargs.keys() if kwargs[v] is None][0]
        else:
            class_name = loader.construct_scalar(node)
        try:
            ObjClass = class_from_string(class_name)
        except Exception:
            raise yaml.MarkedYAMLError(f"`{class_name}` not found",
                                       node.start_mark)
        obj = ObjClass(**kwargs)
        return obj


class BaseConfig(AttrDict):
    def from_file(self, filename: str):
        with open(filename, 'r') as fp:
            self.from_fp(fp)

    def from_fp(self, fp):
        self.from_string(fp.read())

    def from_string(self, data: str):
        config = yaml.load(data, ConfigLoader)
        self.from_dict(config)

    def from_dict(self, config: Dict):
        self.update(config)


class Config(BaseConfig):
    def get_event_pipeline(self, event_type):
        event_config = self.get_event_config(event_type)
        pipeline = event_config.event_pipeline
        if not isinstance(pipeline, EventPipeline):
            pipeline = EventPipeline(pipeline)
        return pipeline

    def get_generators(self, event_type):
        event_config = self.get_event_config(event_type)
        pipeline = event_config.generators
        if not isinstance(pipeline, GenerationPipeline):
            pipeline = GenerationPipeline(pipeline)
        return pipeline

    def get_event_config(self, event_type):
        return AttrDict(self.events[event_type])

    def get_log_format(self):
        return "%(asctime)-15s %(levelname)-7s %(message)s"

    def get_queue_backend(self):
        conf = self.queue
        if not conf:
            raise Exception("No queue configuration")
        backend_name = conf.pop('backend')
        assert backend_name == 'rabbitmq', "No other choice for a while"
        return QueueBackend(**conf)

    def is_aiomonitor_enabled(self):
        # FIXME: only for debug
        return True
