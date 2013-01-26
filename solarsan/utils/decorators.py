
"""
Decorators
"""

from decorator import decorator
import logging
#from django.core.cache import cache


@decorator
def trace(f, *args, **kw):
    print "calling %s with args %s, %s" % (f.func_name, args, kw)
    return f(*args, **kw)


@decorator
def args_list(f, *args, **kwargs):
    if not isinstance(args, list):
        args = isinstance(args, basestring) and [args] or isinstance(args, tuple) and list(args)
    return f(*args, **kwargs)


class conditional_decorator(object):
    """ Applies decorator dec if conditional condition is met """
    def __init__(self, condition, dec, *args, **kwargs):
        self.decorator = dec
        self.decorator_args = (args, kwargs)
        self.condition = condition

    def __call__(self, func):
        if not self.condition:
            # Return the function unchanged, not decorated.
            return func
        return self.decorator(func, *self.decorator_args[0], **self.decorator_args[1])


def statelazyproperty(func):
    """ A decorator for state-based lazy evaluation of properties """
    cache = {}

    def _get(self):
        state = self.__getstate__()
        try:
            v = cache[state]
            logging.debug("Cache hit %s", state)
            return v
        except KeyError:
            logging.debug("Cache miss %s", state)
            cache[state] = value = func(self)
            return value
    return property(_get)
