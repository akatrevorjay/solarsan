
from solarsan import logging, conf
logger = logging.getLogger(__name__)
from lru import LRUCacheDict
import time
import socket
import ethtool
import threading
import zmq

#from zhelpers import zpipe
#from kvmsg import KVMsg
#
#import zmq.utils.jsonapi as json
#try:
#    import cPickle as pickle
#except ImportError:
#    import pickle


INTERVAL = 30
MSG_SIZE = 1


def get_local_ip_to(target):
    ret = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((target, 8000))
        ret = s.getsockname()[0]
        s.close()
    except:
        pass
    return ret


class UDP(object):
    """simple UDP ping class"""
    handle = None   # Socket for send/recv
    broadcast = None  # Broadcast address

    def __init__(self, port, address=None, broadcast=None):
        if broadcast is None:
            broadcast = '255.255.255.255'
        self.broadcast = broadcast
        self.port = port

        # Create UDP socket
        self.handle = socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)

        # Ask operating system to let us do broadcasts from socket
        self.handle.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        # Bind UDP socket to local port so we can receive pings
        self.handle.bind(('', port))

    def send(self, buf):
        self.handle.sendto(buf, 0, (self.broadcast, self.port))

    def recv(self, n):
        buf, addrinfo = self.handle.recvfrom(n)
        print("Found peer %s:%d" % addrinfo)


class Beacon(UDP):
    address = None          # Our own address
    _local_ip_cache = None  # Cache of what our local IP is to target
    _thread = None          # Placeholder for thread is .start() is called

    def __init__(self, interface, port, broadcast=None):
        self.address = ethtool.get_ipaddr(interface)
        self._local_ip_cache = LRUCacheDict(max_size=32, expiration=3600)

        #if not broadcast:
        #    broadcast = ethtool.get_broadcast(interface)

        super(Beacon, self).__init__(port, broadcast=broadcast)

        logger.info("Using iface=%s address=%s:%d broadcast=%s",
                    interface, self.address, self.port, self.broadcast)

    def recv(self, n):
        buf, addrinfo = self.handle.recvfrom(n)
        ip = addrinfo[0]

        try:
            local_ip = self._local_ip_cache[ip]
        except KeyError:
            local_ip = self._local_ip_cache[ip] = get_local_ip_to(ip)
        from_self = local_ip == ip
        if from_self:
            return

        return self.found_peer(ip, buf)

    def send_ping(self):
        logger.debug("Pinging peers...")
        self.send('!')

    def found_peer(self, ip, buf):
        logger.debug("Got peer broadcast ip='%s', buf='%s'", ip, buf)
        # TODO Probe
        logger.error('Not probing peer cause lazy')

    def run(self):
        poller = zmq.Poller()
        poller.register(self.handle, zmq.POLLIN)

        # Send first ping right away
        ping_at = time.time()

        while True:
            timeout = ping_at - time.time()
            if timeout < 0:
                timeout = 0
            try:
                events = dict(poller.poll(1000 * timeout))
            except (KeyboardInterrupt, SystemExit):
                print("interrupted")
                break

            # Someone answered our ping
            if self.handle.fileno() in events:
                self.recv(MSG_SIZE)

            if time.time() >= ping_at:
                # Broadcast our self
                self.send_ping()
                ping_at = time.time() + INTERVAL

    def start(self):
        t = self._thread = threading.Thread(target=self.run)
        t.setName('discovery')
        t.start()

    def join(self, timeout=None):
        if self._thread:
            return self._thread.join(timeout=timeout)


def main():
    beacon = Beacon(conf.config['cluster_iface'], conf.ports['discovery'])

    try:
        beacon.run()
        #beacon.start()
        #beacon.join()
    except (KeyboardInterrupt, SystemExit):
        print("interrupted")


if __name__ == '__main__':
    main()
