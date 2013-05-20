
import gevent


class _BaseManager(gevent.Greenlet):
    channel = None

    def __init__(self, node, **kwargs):
        gevent.Greenlet.__init__(self)
        self.running = False
        self._node = node
        self._set_channel(kwargs.pop('channel', None))
        self._node.add_manager(self)
        self._add_handler()

    def _run(self):
        #self.running = True
        #while self.running:
        #    gevent.sleep(0.1)
        pass

    def _set_channel(self, channel=None):
        if not channel:
            channel = self.channel
        if not channel:
            channel = self.__class__.channel
        if not channel:
            channel = self.__class__.__name__
        self.channel = channel

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
