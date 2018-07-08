"""Utils.
"""
import uuid
import logging
from importlib import import_module


logger = logging.getLogger(__name__)


def gen_id(prefix='', sep='.'):
    """Generate id with prefix and separator.

    Generate ids if form `'{prefix}{separator}{random}'`.
    """
    uniq = uuid.uuid4().hex
    if not prefix:
        return uniq
    return sep.join([prefix, uniq])


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
        try:
            base_module = import_module(base)
            if hasattr(base_module, class_string):
                logger.debug("Class found for %s", class_string)
                return getattr(base_module, class_string)
        except ImportError:
            # no class in base location
            logger.debug("No class found in base location")
        finally:
            tried_paths.append('.'.join([base_module.__name__, class_string]))
    else:
        logger.debug("No base location provided, skip")
    # module.ClassName -> module.ClassName
    parts = class_string.split('.')
    if len(parts) > 1:
        try:
            module = import_module('.'.join(parts[0:-1]))
            return getattr(module, parts[-1])
        except ImportError as e:
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
