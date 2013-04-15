
from solarsan import logging
logger = logging.getLogger(__name__)
#from solarsan.models import CreatedModifiedDocMixIn
from solarsan.models import ReprMixIn
from solarsan.utils.network import cidr_to_netmask, netmask_to_cidr, is_cidr, is_netmask, is_ip, is_ip_in_network, is_domain
#from solarsan.template import quick_template
#import mongoengine as m
from augeas import Augeas

import ipcalc
#import IPy
import netifaces
import pynetlinux
import os
import weakref


"""
Network
"""


def get_interface(name):
    return pynetlinux.ifconfig.Interface(name)


class AugeasWrap(object):
    _transform = 'interfaces'
    _file = None
    _attrs = []
    _map = {}
    _match = None

    __aug = None

    @property
    def _aug(self):
        if not self.__aug:
            self.__aug = Augeas(flags=Augeas.NO_MODL_AUTOLOAD)
            self.__aug.add_transform(self._transform, self._file)
            self.__aug.load()
        return self.__aug

    _debug = False

    def exists(self):
        return bool(self.get())

    def _abspath(self, path):
        if not path or not (path.startswith('/augeas') or path.startswith('/files') or path.startswith('$')):
            path = '%s%s' % (self._match, path or '')
        return path or ''

    def get(self, path=None):
        if self._debug:
            logger.debug('get path=%s', self._abspath(path))
        return self._aug.get(self._abspath(path))

    def set(self, value, path=None):
        value = str(value)
        if self._debug:
            logger.debug('set path=%s value=%s', self._abspath(path), value)
        return self._aug.set(self._abspath(path), value)

    def match(self, path=None):
        if self._debug:
            logger.debug('match path=%s', self._abspath(path))
        return self._aug.match(self._abspath(path))

    def remove(self, path=None):
        if self._debug:
            logger.debug('remove path=%s', self._abspath(path))
        return self._aug.remove(self._abspath(path))

    def insert(self, value, path=None, before=True):
        value = str(value)
        if self._debug:
            logger.debug('insert path=%s value=%s', self._abspath(path), value)
        return self._aug.insert(self._abspath(path), value, before=before)

    def _print(self, path=None):
        path = self._abspath(path)
        get = self.get(path)
        logger.info("[%s] = '%s'", path, get)
        try:
            for match in self.match('%s//*' % path):
                logger.info("[%s] = '%s'", match, self._aug.get(match))
        except RuntimeError:
            pass

    def _all_attrs(self):
        return self._attrs + self._map.keys()


class DebianInterfaceConfig(ReprMixIn, AugeasWrap):
    _transform = 'interfaces'
    _file = '/etc/network/interfaces'
    _match_auto = None

    _attrs = ['family', 'method', 'address', 'netmask', 'gateway', 'mtu']
    _map = {'dns-nameservers': 'nameservers',
            'dns-search': 'search'}

    name = None

    def quick_setup(self, family='inet', method='static', netmask='255.255.255.0', **kwargs):
        if family:
            self.family = family
        if method:
            self.method = method
        if method in ['static'] and netmask:
            self.netmask = netmask
        if kwargs:
            for k, v in kwargs.iteritems():
                setattr(self, k, v)

    @property
    def auto(self):
        return bool(self.get('$ifaces/auto/*[. = "%s"]' % str(self.name)))

    @auto.setter
    def auto(self, value):
        value = bool(value)
        if value is True and not self.auto:
            self.set(str(self.name), '$ifaces/auto[last()+1]/1')
        elif value is False and self.auto:
            self.remove('$ifaces/auto/*[. = "%s"]' % str(self.name))

    @property
    def family(self):
        return self.get('%s/family' % self._match)

    @family.setter
    def family(self, value):
        if not value in ['inet']:
            raise ValueError('%s is not a valid family')
        return self.set(value, '%s/family' % self._match)

    @property
    def method(self):
        return self.get('%s/method' % self._match)

    @method.setter
    def method(self, value):
        if not value in ['dhcp', 'static', 'manual']:
            raise ValueError('%s is not a valid method' % value)
        return self.set(value, '%s/method' % self._match)

    proto = method

    @property
    def ip(self):
        return self.get('%s/address' % self._match)

    @ip.setter
    def ip(self, value):
        if not is_ip(value):
            raise ValueError('%s is not a valid ipaddr' % value)
        return self.set(value, '%s/address' % self._match)

    ipaddr = ip

    @property
    def address(self):
        return '%s/%s' % (self.ipaddr, self.cidr)

    @address.setter
    def address(self, value):
        ipaddr = None
        mask = None
        if '/' in value:
            ipaddr, mask = value.split('/', 1)
        else:
            ipaddr = value
        if ipaddr:
            self.ipaddr = ipaddr
        if mask:
            if is_netmask(mask):
                self.netmask = mask
            elif is_cidr(mask):
                self.cidr = mask
            else:
                raise ValueError('%s is not a valid netmask or cidr' % mask)

    @property
    def netmask(self):
        return self.get('%s/netmask' % self._match)

    @netmask.setter
    def netmask(self, value):
        if not is_netmask(value):
            raise ValueError('%s is not a valid netmask' % value)
        return self.set(value, '%s/netmask' % self._match)

    @property
    def cidr(self):
        netmask = self.netmask
        if not netmask:
            return
        return netmask_to_cidr(netmask)

    @cidr.setter
    def cidr(self, value):
        if not is_cidr(value):
            raise ValueError('%s is not a valid cidr' % value)
        self.netmask = cidr_to_netmask(value)

    @property
    def gateway(self):
        return self.get('%s/gateway' % self._match)

    @gateway.setter
    def gateway(self, value):
        if not is_ip(value):
            raise ValueError('%s is not a valid gateway' % value)
        return self.set(value, '%s/gateway' % self._match)

    @property
    def nameservers(self):
        return self.get('%s/nameservers' % self._match)

    @nameservers.setter
    def nameservers(self, value):
        value = value.split()
        for ns in value:
            if not is_ip(ns):
                raise ValueError('%s is not a valid nameserver' % ns)
        value = ' '.join(value)
        return self.set(value, '%s/nameservers' % self._match)

    @property
    def search(self):
        return self.get('%s/search' % self._match)

    @search.setter
    def search(self, value):
        value = value.split()
        for domain in value:
            if not is_domain(domain):
                raise ValueError('%s is not a valid search domain' % domain)
        value = ' '.join(value)
        return self.set(value, '%s/search' % self._match)

    @property
    def mtu(self):
        mtu = self.get('%s/mtu' % self._match)
        if mtu:
            return int(mtu)

    @mtu.setter
    def mtu(self, value):
        value = int(value)
        return self.set(value, '%s/mtu' % self._match)

    def __init__(self, name_or_nic, replace=False):
        if isinstance(name_or_nic, Nic):
            nic = name_or_nic
        else:
            nic = Nic(name_or_nic)
        self.name = str(nic.name)

        super(DebianInterfaceConfig, self).__init__()
        self.load()

        if replace and self.exists():
            logger.warning('Replacing existing interface config %s due to replace=%s', self, replace)
            logger.warning('Removed %s entires', self.remove())

    def exists_auto(self):
        return bool(self.get(self._match_auto))

    def load(self):
        self._aug.defvar('ifaces', '/files%s' % self._file)
        self._top_node = '$ifaces'
        self._match = '$ifaces/iface[. = "%s"]' % self.name
        self._match_auto = '$ifaces/auto/[* = "%s"]' % self.name

    def save(self):
        if self.gateway and not is_ip_in_network(self.address, self.gateway):
            raise ValueError('Gateway %s is not valid (not in %s)' % (self.gateway, self.address))

        if self.auto is None:
            self.auto = False
        if self.family is None:
            self.family = 'inet'
        if self.method is None:
            self.method = 'static'

        if not self.exists():
            self.set(self.name)
        return self._aug.save()

    @property
    def type(self):
        if self.name.startswith('eth'):
            return 'ethernet'
        elif self.name.startswith('ib'):
            return 'infiniband'
        elif self.name.startswith('lo'):
            return 'local'


class Nic(ReprMixIn):
    name = None

    def __init__(self, name):
        self.name = name
        # This has to be a basestring
        self._obj = get_interface(str(name))

    @property
    def config(self):
        config = None
        if hasattr(self, '_config'):
            config = self._config()
        if config is None:
            #self._config, created = NicConfig.objects.get_or_create(name=self.name)
            config = DebianInterfaceConfig(self)
            self._config = weakref.ref(config)
        return config

    @property
    def broadcast(self):
        net = ipcalc.Network('%s/%s' % (self.ipaddr, self.cidr))
        return str(net.broadcast())

    @property
    def ipaddr(self):
        return self._obj.get_ip()

    @property
    def netmask(self):
        return cidr_to_netmask(self._obj.get_netmask())

    @property
    def cidr(self):
        return self._obj.get_netmask()

    @property
    def mac(self):
        return self._obj.get_mac()

    @property
    def mtu(self):
        fn = '/sys/class/net/%s/mtu' % self.name
        if not os.path.isfile(fn):
            return
        with open(fn) as f:
            return int(f.read())

    '''
    @property
    def addrs(self):
        ret = dict([(netifaces.address_families[x[0]], x[1])
                    for x in netifaces.ifaddresses(self.name).items()
                    ])
        if 'AF_INET6' in ret:
            ## TODO Fix bug in the real issue here, netifaces, where it puts your damn iface name after your IPv6 addr
            inet6_addrs = []
            for addr in ret['AF_INET6']:
                if '%' in addr['addr']:
                    addr['addr'] = addr['addr'][:addr['addr'].index('%')]
                inet6_addrs.append(addr)
            ret['AF_INET6'] = inet6_addrs
        return ret
    '''

    @property
    def addrs(self):
        return netifaces.ifaddresses(self.name)

    @property
    def type(self):
        if self.name.startswith('eth'):
            return 'ethernet'
        elif self.name.startswith('ib'):
            return 'infiniband'
        elif self.name.startswith('lo'):
            return 'local'

    #def get_absolute_url(self):
    #    return reverse('network-interface-detail', kwargs={'slug': self.name})

    def __unicode__(self):
        if self.ipaddr and self.cidr:
            return '%s (%s/%s)' % (self.name, self.ipaddr, self.cidr)
        else:
            return self.name

    @classmethod
    def list(cls):
        ret = {}
        for x in netifaces.interfaces():
            try:
                ret[x] = Nic(x)
            except:
                pass
        #return dict([(x, lambda Nic(x) except: None) for x in netifaces.interfaces()])
        return ret


def get_all_local_ipv4_addrs(nics=None, lo=False):
    if nics is None:
        nics = netifaces.interfaces()
    ret = {}
    for name in nics:
        if not lo and name == 'lo':
            continue
        try:
            addrs = netifaces.ifaddresses(name)[netifaces.AF_INET]
        except KeyError:
            addrs = []
        ret[name] = addrs
    return ret
