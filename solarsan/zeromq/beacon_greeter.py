
from solarsan import logging, conf
log = logging.getLogger(__name__)
from solarsan.utils.stack import get_last_func_name
import solarsan.cluster.models as cmodels

from .beacon import Beacon
from .serializers import pipeline

from functools import partial
from zmq.eventloop.ioloop import IOLoop, DelayedCallback
import zmq
import time
import threading



class Greet:
    @classmethod
    def _gen_from_peer(cls, peer):
        self = Greet()
        self.hostname = peer.hostname
        self.uuid = peer.uuid
        self.cluster_iface = peer.cluster_iface
        return self

    @classmethod
    def gen_from_local(cls):
        peer = cmodels.Peer.get_local()
        return cls._gen_from_peer(peer)

    def __str__(self):
        d = self.__dict__
        data = ''
        for k in ['hostname', 'uuid']:
            v = d.get(k)
            if v:
                data += '%s=%s; ' % (k, v)
        if data:
            data = data[:-2]
        return '<Greet %s>' % data


class Peer:
    ctx = None

    def __init__(self, id, uuid, socket, addr, time_=None, beacon=None, **kwargs):
        self.id = id
        self.uuid = uuid
        self.socket = socket
        self.addr = addr
        self.time = time_ or time.time()

        self.transport, host = addr.split('://', 1)
        self.host, self.beacon_router_port = host.rsplit(':', 1)

        # Set callbacks
        for k, v in kwargs.iteritems():
            if k.startswith('on_') and k.endswith('_cb'):
                setattr(self, k, v)

        if not self.ctx:
            self.ctx = zmq.Context.instance()
        self.loop = IOLoop.instance()

    def _callback(self, name, *args, **kwargs):
        if not name:
            name = get_last_func_name()
            if name.startswith('_on_'):
                name = name[4:]

        meth = getattr(self, 'on_%s' % name, None)
        if meth:
            #meth(*args, **kwargs)
            self.loop.add_callback(partial(meth, *args, **kwargs))

        meth = getattr(self, 'on_%s_cb' % name, None)
        if meth:
            #meth(self, *args, **kwargs)
            self.loop.add_callback(partial(meth, self, *args, **kwargs))

    """
    Greet
    """

    def send_greet(self):
        log.debug('Peer %s: Sending Greet', self.uuid)
        greet = Greet.gen_from_local()
        greet = pipeline.dump(greet)
        #greet = json.dumps(greet.__dict__)
        self.socket.send_multipart(['GREET', greet])

        delay = getattr(self, 'send_greet_delay', None)
        if delay:
            delay.stop()
            delattr(self, 'send_greet_delay')

    def _on_greet(self, serialized_obj):
        greet = pipeline.load(serialized_obj)
        #greet = Greet()
        #greet.__dict__ = json.loads(serialized_obj)

        log.debug('Peer %s: Got GREET: %s', self.uuid, greet)

        self.greet = greet

        return self._callback(None, serialized_obj)


class GreeterBeacon(Beacon):
    _peer_cls = Peer

    def __init__(self, *args, **kwargs):
        Beacon.__init__(self, *args, **kwargs)

    def start(self, loop=True):
        Beacon.start(self, loop=loop)

    def on_recv_msg(self, peer, *msg):
        cmd = msg[0]
        log.debug('Peer %s: %s.', peer.uuid, cmd)

        if cmd == 'GREET':
            peer._on_greet(msg[1])
            self.loop.add_callback(partial(self._callback, 'peer_on_greet', peer))
        else:
            log.error('Peer %s: Wtfux %s?', peer.uuid, cmd)
            peer.socket.close()

    def on_peer_connected(self, peer):
        log.info('Peer %s: Connected.', peer.uuid)

        peer.send_greet_delay = DelayedCallback(peer.send_greet, self.beacon_interval * 1000)
        peer.send_greet_delay.start()

    def on_peer_lost(self, peer):
        log.info('Peer %s: Lost.', peer.uuid)


def main():
    local = cmodels.Peer.get_local()
    ipaddr = str(local.cluster_nic.ipaddr)

    log.info('Starting Beacon (service_addr=%s, discovery_port=%s)',
             ipaddr, conf.ports.discovery)

    gb = GreeterBeacon(
        #beacon_interval=2,
        beacon_interval=1,
        dead_interval=10,
        service_addr=ipaddr,
        #broadcast_addr=bcast,
        broadcast_port=conf.ports.discovery,
    )

    try:
        #gb.start()

        t = threading.Thread(target=gb.start)
        t.daemon = True
        t.start()
        log.info('Started.')

        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        pass

if __name__ == '__main__':
    main()
