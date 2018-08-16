"""Utils.
"""
import uuid
import logging
from importlib import import_module

from typing import Dict, Iterable


logger = logging.getLogger(__name__)


def gen_id(prefix='', sep='.'):
    """Generate id with prefix and separator.

    Generate ids if form `'{prefix}{separator}{random}'`.
    """
    uniq = uuid.uuid4().hex
    if not prefix:
        return uniq
    return sep.join([prefix, uniq])


def short_id(some_id, length=8, right_add=0, sep='..'):
    """Make short id for logging.
    """
    half = int(length / 2)
    return sep.join([
        some_id[:half],
        some_id[-half-right_add:]
    ])


def class_from_string(class_string, base=None):
    """Get class from string.

    Use base to search in default module.
    """
    # ClassName -> <base>.ClassName
    # module.ClassName -> <base>.module.ClassName
    tried_paths = []
    logger.debug(
        "Try to find class for `%s` with base %s",
        class_string, base
    )
    if base is not None:
        base_module = None
        try:
            base_module = import_module(base)
            if hasattr(base_module, class_string):
                logger.debug("Class found for %s", class_string)
                return getattr(base_module, class_string)
        except ImportError:  # pragma: no cover
            # no class in base location
            logger.debug("No class found in base location")
        finally:
            # collect search paths for logs
            name = base_module.__name__ if base_module else None
            segments = filter(lambda x: x, [name, class_string])
            tried_paths.append('.'.join(segments))
    else:
        logger.debug("No base location provided, skip")
    # module.ClassName -> module.ClassName
    parts = class_string.split('.')
    if len(parts) > 1:
        try:
            module = import_module('.'.join(parts[0:-1]))
            return getattr(module, parts[-1])
        except ImportError as e:  # pragma: no cover
            logger.error(
                'Cant find %s %s %s', '.'.join(parts[0:-1]), parts[-1], e
            )
        finally:
            tried_paths.append(class_string)
    else:
        logger.debug(
            "There is no module path in `%s` identifier.",
            class_string
        )

    raise Exception("Can't find class %s. Base: %s Tried: %s" % (
        parts[-1],
        base,
        ', '.join(tried_paths)
    ))


class Serializable:

    """Serializable mixin.
    """

    name: str
    args: Iterable
    kwargs: Dict

    def serialize(self):
        """Get serialized representation of effect.
        """
        return (
            self.serialize_type(),
            self.serialize_args(),
            self.serialize_kwargs()
        )

    def serialize_type(self):
        """Serialize type to string.
        """
        return '.'.join([
            self.__class__.__module__,
            self.__class__.__name__
        ])

    def serialize_args(self):
        """Serialize effect arguments.
        """
        return self.args

    def serialize_kwargs(self):
        """Serialize effect keyword arguments.
        """
        return self.kwargs

    @classmethod
    def load(cls, args, kwargs):
        """Load this effect using provided args and kwargs.
        """
        return cls(*cls.load_args(args), **cls.load_kwargs(kwargs))

    @classmethod
    def load_args(cls, args):
        """Deserialize arguments.
        """
        return args  # pragma: no cover

    @classmethod
    def load_kwargs(cls, kwargs):
        """Deserialize keyword arguments.
        """
        return kwargs


class NamedSerializable(Serializable):
    """Named serializable.

    Use `name` value when serializing instance type information instead of full
    class path.
    """
    name: str

    def serialize_type(self):
        return self.name
