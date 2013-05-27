
import zmq


class Socket(zmq.Socket):

    def dict_configure(self, data):
        if 'bind' in data:
            if isinstance(data['bind'], (tuple, list)):
                for addr in data['bind']:
                    self.bind(addr)
            else:
                self.bind(data['bind'])
        if 'connect' in data:
            if isinstance(data['connect'], (tuple, list)):
                for addr in data['connect']:
                    self.connect(addr)
            else:
                self.connect(data['connect'])
        # we have no identity as swap as they will be dropped
        if 'hwm' in data:
            self.setsockopt(zmq.HWM, int(data['hwm']))
        if 'affinity' in data:
            self.setsockopt(zmq.AFFINITY, int(data['affinity']))
        if 'backlog' in data:
            self.setsockopt(zmq.BACKLOG, int(data['backlog']))
        if 'linger' in data:
            self.setsockopt(zmq.LINGER, int(data['linger']))
        if 'sndbuf' in data:
            self.setsockopt(zmq.SNDBUF, int(data['sndbuf']))
        if 'rcvbuf' in data:
            self.setsockopt(zmq.RCVBUF, int(data['rcvbuf']))
        # TODO(tailhook) add other options
