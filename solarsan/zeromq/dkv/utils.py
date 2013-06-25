
from solarsan import logging
logger = logging.getLogger(__name__)
import re


def get_endpoint(transport=None, host=None, port=None, service=None):
    if port is not None:
        port = ':%s' % int(port)
    if service is not None:
        service = '/%s' % service
    return '%s://%s%s%s' % (transport, host or '', port or '', service or '')


def parse_endpoint(endpoint):
    m = re.match(
        r'^(?P<transport>\w+)://(?P<host>[-\*\w\.]+)(?P<port>:\d+)?(?P<service>/.*)?$', endpoint,
        re.IGNORECASE)
    if not m:
        raise ValueError('Invalid endpoint %s', endpoint)
    ret = m.groupdict()

    if ret['port']:
        ret['port'] = int(ret['port'][1:])
    if ret['service']:
        ret['service'] = ret['service'][1:]

    return ret


class ZmqEndpoint:

    def __init__(self, *args, **kwargs):
        if 'socket_type' in kwargs:
            self.socket_type = kwargs.pop('socket_type')

        if args:
            if len(args) == 1 and isinstance(args[0], basestring):
                endpoint = args[0]
            else:
                endpoint = get_endpoint(*args)
        elif kwargs:
            if 'endpoint' in kwargs:
                endpoint = kwargs['endpoint']
            else:
                endpoint = get_endpoint(**kwargs)

        data = parse_endpoint(endpoint)

        self.__dict__.update(data)
        self.endpoint = endpoint

    def __str__(self):
        return self.endpoint

    def __repr__(self):
        return "<%s '%s'>" % (self.__class__.__name__, self.endpoint)

    def as_dict(self):
        return self.__dict__
