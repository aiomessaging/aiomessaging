"""
dummy filters
"""


class NoopFilter:
    """No-op filter.

    For testing.
    """
    def __init__(self, *args, **kwargs):
        pass

    def __call__(self, event):
        pass
