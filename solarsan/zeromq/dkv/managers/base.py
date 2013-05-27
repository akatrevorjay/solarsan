
from solarsan import logging, LogMixin
logger = logging.getLogger(__name__)

import gevent
#from datetime import datetime
#from functools import partial
#import weakref

#from .channel import Channel
from reflex.base import Reactor


"""
Base
"""


class NodePlugin(type):
    _registry = set()

    def __new__(meta, name, bases, dct):
        ret = type.__new__(meta, name, bases, dct)
        meta._registry.add(ret)
        return ret

    #def __init__(cls, name, bases, dct):
    #    dct['log'] = logging.getLogger('%s.%s' % (_get_caller_module_name(), name))
    #    super(LogMetaAttr, cls).__init__(name, bases, dct)

    #def __call__(cls, *args, **kwargs):
    #    if 'log' not in kwargs:
    #        kwargs['log'] = logging.getLogger('%s.%s' % (_get_caller_module_name(), cls.__name__))
    #    return type.__call__(cls, *args, **kwargs)

    #class __metaclass__(type):
    #    def __init__(cls, name, bases, dict):
    #        NodePlugin._registry.add((name, cls))
    #        return type.__init__(name, bases, dict)


## in your plugin modules
#class SpamPlugin(Plugin):
#    pass

#class BaconPlugin(Plugin):
#    pass

## in your plugin loader
## import all plugin modules

## loop over registered plugins
#for name, cls in registry:
#    if cls is not Plugin:
#    print name, cls



class _BaseManager(gevent.Greenlet, Reactor, LogMixin):
    channel = None

    def __init__(self, node, **kwargs):
        if hasattr(self, 'pre_init'):
            self.pre_init(node, **kwargs)

        gevent.Greenlet.__init__(self)

        self._node = node
        self._set_channel(kwargs.pop('channel', None))
        self._node.add_manager(self)
        self._add_handler()

        # This actually calls self.init
        Reactor.__init__(self, node.events, node, **kwargs)

        if hasattr(self, 'post_init'):
            self.post_init(node, **kwargs)

    def init(self, node, **kwargs):
        pass

    def _set_channel(self, channel=None):
        if not channel:
            channel = self.channel
        if not channel:
            channel = self.__class__.channel
        if not channel:
            channel = self.__class__.__name__
            if channel.endswith('Manager'):
                channel = channel.rsplit('Manager', 1)[0]
        self.channel = channel

    """ Run """

    tick_length = 1
    tick_timeout = None

    def _tick(self):
        """What gets ran at each tick length in the _run loop."""
        pass

    def _run(self):
        """What gets spawned to run this manager. Defaults to a quick ticker that spawns
        self._tick every self.time_length seconds, with a timeout of self.time_timeout.
        """
        self.running = True
        tick_length = getattr(self, 'tick_length')
        tick_timeout = getattr(self, 'tick_timeout')

        while self.running:
            gevent.sleep(tick_length)

            g = gevent.spawn(self._tick)
            g.join(timeout=tick_timeout)

            if not g.dead:
                self.log.error('Tick timed out.')
                g.kill()

    """ Handlers """

    def _add_handler(self, channel=None):
        if not channel:
            channel = self.channel
        self._node.add_handler(channel, self)

    def _remove_handler(self, channel=None):
        if not channel:
            channel = self.channel
        self._node.remove_handler(channel, self)

    """ Abstractions """

    def broadcast(self, message_type, *parts, **kwargs):
        channel = kwargs.pop('channel', self.channel)
        return self._node.broadcast(channel, message_type, *parts)

    def unicast(self, peer, message_type, *parts, **kwargs):
        channel = kwargs.pop('channel', self.channel)
        return self._node.unicast(peer, channel, message_type, *parts)
