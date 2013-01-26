
"""
Singleton
"""


class Borg:
    __shared_state = {}

    def __init__(self, *args, **kwargs):
        self.__dict__ = self.__shared_state
        super(Borg, self).__init__(self, *args, **kwargs)


class Singleton:
    __single = None

    def __init__(self):
        if Singleton.__single:
            raise Singleton.__single
        Singleton.__single = self

    @classmethod
    def getinstance(cls, x=None):
        if x is None:
            x = cls
        try:
            single = x()
        except Singleton, s:
            single = s
        return single


class SingletonDecorator:
    """
    A non-thread-safe helper class to ease implementing singletons.
    This should be used as a decorator -- not a metaclass -- to the
    class that should be a singleton.

    The decorated class can define one `__init__` function that
    takes only the `self` argument. Other than that, there are
    no restrictions that apply to the decorated class.

    To get the singleton instance, use the `Instance` method. Trying
    to use `__call__` will result in a `TypeError` being raised.

    Limitations: The decorated class cannot be inherited from.

    """

    def __init__(self, decorated):
        self._decorated = decorated

    def Instance(self):
        """
        Returns the singleton instance. Upon its first call, it creates a
        new instance of the decorated class and calls its `__init__` method.
        On all subsequent calls, the already created instance is returned.

        """
        try:
            return self._instance
        except AttributeError:
            self._instance = self._decorated()
            return self._instance

    def __call__(self):
        raise TypeError('Singletons must be accessed through `Instance()`.')

    def __instancecheck__(self, inst):
        return isinstance(inst, self._decorated)


class SingletonMeta(type):
    """
    Example:
        class MyClass(object):
            __metaclass__ = SingletonMeta
    """
    def __init__(cls, name, bases, dict):
        super(Singleton, cls).__init__(name, bases, dict)
        cls.instance = None

    def __call__(cls, *args, **kw):
        if cls.instance is None:
            cls.instance = super(Singleton, cls).__call__(*args, **kw)
        return cls.instance


def singleton_pep318(cls):
    instances = {}

    def getinstance():
        if cls not in instances:
            instances[cls] = cls()
        return instances[cls]
    return getinstance

"""
class NothingSpecial:
    pass

_the_one_and_only = None

def TheOneAndOnly():
    global _the_one_and_only
    if not _the_one_and_only:
        _the_one_and_only = NothingSpecial()
    return _the_one_and_only
"""


"""
def getSystemContext(contextObjList=[]):
    if len( contextObjList ) == 0:
        contextObjList.append( Context() )
        pass
    return contextObjList[0]

class Context(object):
    # Anything you want here
"""
