
from solarsan import logging, conf
logger = logging.getLogger(__name__)
from solarsan.exceptions import SolarSanError
import re


def get_address(host='localhost', port=None,
                service='', transport='tcp'):
    if not host:
        host = 'localhost'
    if not port:
        port = conf.ports.dkv
    if not transport:
        transport = 'tcp'
    if not service:
        service = ''
    return '%s://%s:%s%s' % (transport, host, port, service)


def parse_address(address):
    m = re.match(r'^(?P<transport>\w+)://(?P<host>[-\w\.]+)(?P<port>:\d+)?(?P<service>/.*)?$', address, re.IGNORECASE)
    if not m:
        raise SolarSanError('Invalid address %s', address)
    ret = m.groupdict()
    ret['port'] = int(ret.get('port') and ret['port'][1:] or conf.ports.dkv)
    return ret
