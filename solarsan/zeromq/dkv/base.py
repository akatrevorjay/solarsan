
class _BaseDict(dict):
    def __init__(self, **kwargs):
        dict.__init__(self, **kwargs)

    def __repr__(self):
        return '<%s "%s">' % (self.__class__.__name__, dict.__repr__(self))

    def __setattr__(self, k, v):
        if k.startswith('_'):
            return object.__setattr__(self, k, v)
        return dict.__setitem__(self, k, v)

    def __getattr__(self, k):
        if k.startswith('_'):
            return object.__getattribute__(self, k)
        try:
            return dict.__getitem__(self, k)
        except KeyError:
            raise AttributeError


