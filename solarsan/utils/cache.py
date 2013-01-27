"""
DefaultDictCache/QuerySetCache
"""

from collections import defaultdict


class DefaultDictCache(defaultdict):
    def __missing__(self, key):
        value = self.get_missing_key(key)
        self[key] = value
        return value


class QuerySetCache(DefaultDictCache):
    def __init__(self, *args, **kwargs):
        for k in ['objects', 'document', 'query_kwarg']:
            if k in kwargs:
                v = kwargs.pop(k)
                setattr(self, k, v)
        if getattr(self, 'document', None):
            self.objects = self.document.objects
        self.query_kwarg = kwargs.pop('query_kwarg', 'name')
        return super(QuerySetCache, self).__init__(*args, **kwargs)

    def get_kwargs(self, key, **kwargs):
        return {self.query_kwarg: key, }

    def get_missing_key(self, key):
        kwargs = self.get_kwargs(key)
        return self.objects.get_or_create(**kwargs)[0]


"""
Cache Helpers
"""
'''
from django.core.cache import cache
from django.conf import settings
import random


class CacheDict(dict):
    prefix = 'cachedict'
    sep = '__'
    timeout = 15

    def __getitem__(self, key):
        value = self.get(key)
        if not value:
            raise KeyError
        return value

    def __setitem__(self, key, value):
        self.set(key, value)

    def _prep_key(self, key):
        sep = None
        if self.prefix:
            sep = self.sep
        return self.prefix + sep + key

    def get(self, key, default_value=None, version=None):
        key = self._prep_key(key)
        return cache.get(key, default_value, version=version)
        #return cache.get(key, default_value)

    def set(self, key, value, timeout=None, version=None):
        key = self._prep_key(key)
        if not timeout:
            timeout = self.timeout
            if hasattr(timeout, '__call__'):
                timeout = timeout(key)
        cache.set(key, value, timeout=timeout, version=version)
        #cache.set(key, value, timeout=timeout)

    def delete(self, key, version=None):
        key = self._prep_key(key)
        return cache.delete(key, version=version)
        #return cache.delete(key)

    def incr_version(self, key):
        key = self._prep_key(key)
        return cache.incr_version(key)

    def decr_version(self, key):
        key = self._prep_key(key)
        return cache.decr_version(key)



class RandTimeoutRangeCacheDict(CacheDict):
    # One minute for dev, 5 minutes for prod
    timeout_min = settings.DEBUG and 60 or 300
    timeout_rand_range = [1, 10]
    timeout = lambda self, key: self.timeout_min + random.randint(self.timeout_rand_range[0], self.timeout_rand_range[1])
'''

"""
Cache
"""

class Memoize(object):
    """
    Cached function or property

    >>> import random
    >>> @CachedFunc( ttl=3 )
    >>> def cachefunc_tester( *args, **kwargs ):
    >>>     return random.randint( 0, 100 )

    """
    __name__ = "<unknown>"
    def __init__( self, func=None, ttl=300 ):
        self.ttl = ttl
        self.__set_func( func )
    def __set_func( self, func=None, doc=None ):
        if not func:
            return False
        self.func = func
        self.__doc__ = doc or self.func.__doc__
        self.__name__ = self.func.__name__
        self.__module__ = self.func.__module__
    def __call__( self, func=None, doc=None, *args, **kwargs ):
        if func:
            self.__set_func( func, doc )
            return self
        now = time.time()
        try:
            value, last_update = self._cache
            if self.ttl > 0 and now - last_update > self.ttl:
                raise AttributeError
        except ( KeyError, AttributeError ):
            value = self.func( *args, **kwargs )
            self._cache = ( value, now )
        return value
    def __get__( self, inst, owner ):
        now = time.time()
        try:
            value, last_update = inst._cache[self.__name__]
            if self.ttl > 0 and now - last_update > self.ttl:
                raise AttributeError
        except ( KeyError, AttributeError ):
            value = self.func( inst )
            try:
                cache = inst._cache
            except AttributeError:
                cache = inst._cache = {}
            cache[self.__name__] = ( value, now )
        return value
    def __repr__( self ):
        return "<@CachedFunc: '%s'>" % self.__name__


import time


class cached_property(object):
    '''Decorator for read-only properties evaluated only once within TTL period.

    It can be used to created a cached property like this::

        import random

        # the class containing the property must be a new-style class
        class MyClass(object):
            # create property whose value is cached for ten minutes
            @cached_property(ttl=600)
            def randint(self):
                # will only be evaluated every 10 min. at maximum.
                return random.randint(0, 100)

    The value is cached  in the '_cache' attribute of the object instance that
    has the property getter method wrapped by this decorator. The '_cache'
    attribute value is a dictionary which has a key for every property of the
    object which is wrapped by this decorator. Each entry in the cache is
    created only when the property is accessed for the first time and is a
    two-element tuple with the last computed property value and the last time
    it was updated in seconds since the epoch.

    The default time-to-live (TTL) is 300 seconds (5 minutes). Set the TTL to
    zero for the cached value to never expire.

    To expire a cached property value manually just do::

        del instance._cache[<property name>]

    '''
    def __init__(self, ttl=300):
        self.ttl = ttl

    def __call__(self, fget, doc=None):
        self.fget = fget
        self.__doc__ = doc or fget.__doc__
        self.__name__ = fget.__name__
        self.__module__ = fget.__module__
        return self

    def __get__(self, inst, owner):
        now = time.time()
        try:
            value, last_update = inst._cache[self.__name__]
            if self.ttl > 0 and now - last_update > self.ttl:
                raise AttributeError
        except (KeyError, AttributeError):
            value = self.fget(inst)
            try:
                cache = inst._cache
            except AttributeError:
                cache = inst._cache = {}
            cache[self.__name__] = (value, now)
        return value


# note that this decorator ignores **kwargs
def memoize(obj):
    cache = obj.cache = {}

    @functools.wraps(obj)
    def memoizer(*args, **kwargs):
        if args not in cache:
            cache[args] = obj(*args, **kwargs)
        return cache[args]
    return memoizer


import collections
import functools


class memoized(object):
   '''Decorator. Caches a function's return value each time it is called.
   If called later with the same arguments, the cached value is returned
   (not reevaluated).
   '''
   def __init__(self, func):
      self.func = func
      self.cache = {}
   def __call__(self, *args):
      if not isinstance(args, collections.Hashable):
         # uncacheable. a list, for instance.
         # better to not cache than blow up.
         return self.func(*args)
      if args in self.cache:
         return self.cache[args]
      else:
         value = self.func(*args)
         self.cache[args] = value
         return value
   def __repr__(self):
      '''Return the function's docstring.'''
      return self.func.__doc__
   def __get__(self, obj, objtype):
      '''Support instance methods.'''
      return functools.partial(self.__call__, obj)
