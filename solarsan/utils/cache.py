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
