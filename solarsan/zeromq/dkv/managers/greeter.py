
from .base import _BaseManager
import gevent
#from reflex.data import Event


class Greeter(_BaseManager):

    debug = False

    # TODO event on connect, use that to start sending greets
    # TODO Start greeter manager ticks, send greets every second until
    # we get one back.

    def __init__(self, node):
        _BaseManager.__init__(self, node)

        self.bind(self._on_peer_connected, 'peer_connected')
        # self.bind(self._on_peer_syncing, 'peer_syncing')

        self._node.greeter = self

    def _on_peer_connected(self, event, peer):
        self.log.debug('Event: %s is connected', peer)
        gevent.spawn(self.greet, peer)
        #gevent.spawn(self.greet_loop, peer)

    """ Greet """

    def greet_loop(self, peer, is_reply=False, timeout=10):
        peer._greeter_running = True
        x = 0
        while getattr(peer, '_greeter_running', None):
            self.greet(peer, is_reply)
            gevent.sleep(1)
            if is_reply:
                break
            if bool(timeout) and x > timeout:
                # TODO Why won't peers DIE
                peer.shutdown()
                break
            x += 1
        if hasattr(peer, '_greeter_running'):
            delattr(peer, '_greeter_running')

    def _on_peer_syncing(self, event, peer):
        self.log.debug('Peer %s is syncing, stopping greeting')

        if hasattr(peer, '_greeter_running'):
            peer._greeter_running = False

    def greet(self, peer, is_reply=False, timeout=10):
        self.log.debug('Greeting %s is_reply=%s', peer, is_reply)
        self.unicast(peer, 'greet', is_reply, self._node.uuid)
        #gevent.sleep(1)

    def receive_greet(self, peer, is_reply, node_uuid, *args, **kwargs):
        # Temp hackery for debug log
        args = [is_reply, node_uuid]
        args.extend(args)

        self.log.debug(
            'Received greet peer=%s args=%s kwargs=%s channel=%s', peer, args, kwargs, self.channel)

        peer.receive_greet()

        if not is_reply:
            gevent.spawn(self.greet, peer, is_reply=True)
