
from .base import _BaseManager
import gevent
from reflex.data import Event


class Greeter(_BaseManager):

    debug = False

    # TODO event on connect, use that to start sending greets
    # TODO Start greeter manager ticks, send greets every second until
    # we get one back.

    def __init__(self, node):
        _BaseManager.__init__(self, node)

        self.bind(self._on_peer_connected, 'peer_connected')

        self._node.greeter = self

    def _on_peer_connected(self, event, peer):
        self.log.debug('Event: %s is connected', peer)
        gevent.spawn(self.greet, peer)

    """ Greet """

    def greet(self, peer, is_reply=False):
        self.log.debug('Greeting %s is_reply=%s', self, is_reply)
        self.unicast(peer, 'greet', is_reply, self._node.uuid)

    def receive_greet(self, peer, is_reply, node_uuid, *args, **kwargs):
        # Temp hackery for debug log
        args = [is_reply, node_uuid]
        args.extend(args)

        channel = self.channel
        self.log.debug('Received greet peer=%s args=%s kwargs=%s channel=%s', peer, args, kwargs, channel)

        peer.receive_greet()

        #if not is_reply:
        if True:
            self.greet(peer, is_reply=True)
