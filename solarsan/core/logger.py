
#from solarsan import logging
#logger = logging.getLogger(__name__)

import logging
import logging.config
from ..conf import LOGGING
from stacklogger import StackLogger
import sys
import inspect


"""
Logger
"""


class Logger(StackLogger):
    __call__ = StackLogger.debug


"""
Augmented Stack Aware Logging
"""


class AugmentedLogger(logging.getLoggerClass()):
    """A simple augmented logger that allows you to specify an arbitrary
    stack-depth when logging relative to the caller function.

    import contextlib
    import time
    import sys

    @contextlib.contextmanager
    def timer(name):
        t1 = time.time()
        yield
        total_time = time.time() - t1

        # The depth is set to 2 since we are technically in the __exit__ and we
        # want to go up through exit and to the parent function.
        logger.debug('{}: {}ms'.format(name, total_time), depth=2)


    def sum_n(n):
        with timer('summing range'):
            logger.info('total: {}'.format(sum(range(n))))

    if __name__ == '__main__':
        logging.basicConfig(format='%(funcName)s:%(lineno)s %(message)s')
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

        n = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
        sum_n(n)
    """
    def _log(self, l, msg, args, exc=None, extra=None, stack=False, depth=0):
        # We look-up down the stack to the caller function and pull the
        # necessary meta-data for this call. This is not guaranteed to work
        # for all types of Python--consult the actual logging module for some
        # insight into how they handle this for IronPython.
        stack = inspect.stack()
        assert len(stack) > 2 + depth
        #frame, filename, lineno, function, *other = stack[2 + depth]
        frame, filename, lineno, function = stack[2 + depth][:4]
        rec_args = (self.name, l, filename, lineno, msg,
                    args, exc, function, extra, inspect.trace())
        record = self.makeRecord(*rec_args)
        self.handle(record)


def _get_caller_module_name():
    def inner():
        return sys._getframe(3)
    f = None
    m = None
    ret = None
    try:
        f = inner()
        m = inspect.getmodule(f)
        ret = m.__name__
    except:
        pass
    finally:
        del f
        del m
    return ret



class LogMetaKwargs(type):
    """Metaclass to stuff class level logger into self.log of new instance.
    """
    def __call__(cls, *args, **kwargs):
        if 'log' not in kwargs:
            kwargs['log'] = logging.getLogger('%s.%s' % (_get_caller_module_name(), cls.__name__))
        return type.__call__(cls, *args, **kwargs)


class LogMetaAttr(type):
    """Metaclass to stuff class level logger into self.log of new instance.
    """
    def __new__(meta, name, bases, dct):
        dct['log'] = logging.getLogger('%s.%s' % (_get_caller_module_name(), name))
        return type.__new__(meta, name, bases, dct)

    #def __init__(cls, name, bases, dct):
    #    dct['log'] = logging.getLogger('%s.%s' % (_get_caller_module_name(), name))
    #    super(LogMetaAttr, cls).__init__(name, bases, dct)

    #def __call__(cls, *args, **kwargs):
    #    if 'log' not in kwargs:
    #        kwargs['log'] = logging.getLogger('%s.%s' % (_get_caller_module_name(), cls.__name__))
    #    return type.__call__(cls, *args, **kwargs)


LogMeta = LogMetaAttr


#class LogMixin(object):
#    __metaclass__ = LogMeta
#
#    def __logmeta_init(self, log):
#        self.log = log
#
#    #def __init__(self, *args, **kwargs):
#    #    super(LogMixin, self).__init__(self, *args, **kwargs)


class LogMixin(object):
    @property
    def log(self):
        if not hasattr(self, '__log'):
            self.__log = logging.getLogger('%s.%s' % (_get_caller_module_name(), self.__class__.__name__))
        return self.__log


#class _FakeLog(object):
#
#    def debug(self):
#        pass
#
#_fake_log = _FakeLog()
#log = _fake_log


"""
Local Context Filter
"""

# gevent should work through here as well
from threading import local


class LocalContextFilter(object):  # (logging.Filter):
    """
    This is a filter which injects contextual information into the log.
    """
    def filter(self, record):
        loc = local()

        if hasattr(loc, 'logctx'):
            ctx = loc.logctx
            # for k, v in ctx:
            #    setattr(record, k, v)
        else:
            ctx = None

        record.context = '({})'.format(ctx)

        return True


"""
Main
"""


#logging.setLoggerClass(AugmentedLogger)
logging.setLoggerClass(Logger)
logging.config.dictConfig(LOGGING)
#root = logging.getLogger()
#logger = logging.getLogger(__name__)


#from .conf import config

#from solarsan import conf
#import logging
#import logging.config

#root = logging.getLogger()
#logger = logging.getLogger('solarsan')

#logging.config.dictConfig(conf.LOGGING)

#logger = logging.getLogger('solarsan')

#formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s.%(module)s %(message)s @%(funcName)s:%(lineno)d')
##formatter = logging.Formatter('%(name)s.%(module)s/%(processName)s[%(process)d]: [%(levelname)s] %(message)s @%(funcName)s:%(lineno)d')
#sl_formatter = logging.Formatter('%(name)s.%(module)s/%(processName)s[%(process)d]: %(message)s @%(funcName)s:%(lineno)d')
##sl_formatter = logging.Formatter('solarsan/%(name)s.%(module)s/%(processName)s[%(process)d]: %(message)s @%(funcName)s:%(lineno)d')

#logger = logging.getLogger('solarsan')
#logger.setLevel(logging.DEBUG)

#ch = logging.StreamHandler()
#ch = ConsoleHandler()
#ch.setLevel(logging.DEBUG)

#formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s.%(module)s %(message)s @%(funcName)s:%(lineno)d')
#ch.formatter = formatter

#logger.addHandler(ch)

#sl = SysLogHandler(address='/dev/log')
#sl.setLevel(logging.DEBUG)
#sl.formatter = sl_formatter
##logger.addHandler(sl)
