
from solarsan import logging
logger = logging.getLogger(__name__)
#from solarsan.models import CreatedModifiedDocMixIn
from solarsan.models import ReprMixIn
#from solarsan.template import quick_template
#import mongoengine as m
from augeas import Augeas

import ipcalc
import IPy
import netifaces
import pynetlinux
import os
import weakref


"""
Network
"""


#CIDR_CHOICES = (
#    (1, '128.0.0.0'),
#    (2, '192.0.0.0'),
#    (3, '224.0.0.0'),
#    (4, '240.0.0.0'),
#    (5, '248.0.0.0'),
#    (6, '252.0.0.0'),
#    (7, '254.0.0.0'),
#    (8, '255.0.0.0'),
#    (9, '255.128.0.0'),
#    (10, '255.192.0.0'),
#    (11, '255.224.0.0'),
#    (12, '255.240.0.0'),
#    (13, '255.248.0.0'),
#    (14, '255.252.0.0'),
#    (15, '255.254.0.0'),
#    (16, '255.255.0.0'),
#    (17, '255.255.128.0'),
#    (18, '255.255.192.0'),
#    (19, '255.255.224.0'),
#    (20, '255.255.240.0'),
#    (21, '255.255.248.0'),
#    (22, '255.255.252.0'),
#    (23, '255.255.254.0'),
#    (24, '255.255.255.0'),
#    (25, '255.255.255.128'),
#    (26, '255.255.255.192'),
#    (27, '255.255.255.224'),
#    (28, '255.255.255.240'),
#    (29, '255.255.255.248'),
#    (30, '255.255.255.252'),
#    (31, '255.255.255.254'),
#    (32, '255.255.255.255'),
#)


#def rev_dub_tup(iterator):
#    iterator = dict(iterator)
#    return dict(zip(iterator.values(), iterator.keys()))

#NETMASK_CHOICES = rev_dub_tup(CIDR_CHOICES)


def convert_cidr_to_netmask(arg):
    return str(IPy.IP('0/%s' % arg, make_net=True).netmask())


def convert_netmask_to_cidr(arg):
    return int(IPy.IP('0/%s' % arg, make_net=True).prefixlen())


def get_interface(name):
    return pynetlinux.ifconfig.Interface(name)


#class NicConfig(ReprMixIn, m.Document, CreatedModifiedDocMixIn):
#    mac = m.StringField()
#    name = m.StringField()
#    PROTO_CHOICES = (
#        ('none', 'Disabled'),
#        ('static', 'Static IP'),
#        ('dhcp', 'DHCP'),
#    )
#    proto = m.StringField()
#    ipaddr = m.StringField()
#    netmask = m.StringField()
#    mtu = m.IntField()
#
#    gateway = m.StringField()
#
#    is_enabled = m.BooleanField()


class AugeasWrap(object):
    _file = None
    _attrs = []
    _map = {}

    def __init__(self):
        self._aug = Augeas()

    def exists(self):
        #return bool(self._aug.get(self._match))
        return bool(self.get())
        #return bool(self.match())

    def _abspath(self, path):
        if not path or not (path.startswith('/augeas') or path.startswith('/files') or path.startswith('$')):
            path = '%s%s' % (self._match, path or '')
        return path or ''

    def get(self, path=None):
        logger.debug('get path=%s', self._abspath(path))
        return self._aug.get(self._abspath(path))

    def set(self, value, path=None):
        logger.debug('set path=%s value=%s', self._abspath(path), value)
        return self._aug.set(self._abspath(path), value)

    def match(self, path=None):
        logger.debug('match path=%s', self._abspath(path))
        return self._aug.match(self._abspath(path))

    def remove(self, path=None):
        logger.debug('remove path=%s', self._abspath(path))
        return self._aug.remove(self._abspath(path))

    def insert(self, value, path=None, before=True):
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


'''
There is a bug with this that I'm asking about on IRC.

Took me forever to figure out, but it cannot write an iface alias to interfaces.
It can however, read and mostly edit it.


* I manually put it in the file before parsing:

    In [1]: from configure.models import Nic, DebianInterfaceConfig

    In [2]: b = DebianInterfaceConfig('eth1:ha0')

    In [3]: b.address
    2013-04-13 15:43:03,309 DEBUG configure.models@get:115 get path=$ifaces/iface[. = "eth1:ha0"]/address
    Out[3]: u'10.90.90.50'

    In [4]: b.save()

    In [5]:

    In [5]:


* Failure:

In [1]: from configure.models import Nic, DebianInterfaceConfig

In [2]: b = DebianInterfaceConfig('eth1:ha0')
b
In [3]: b.quick_setup(address='10.90.90.50')
2013-04-13 15:45:02,730 DEBUG configure.models@set:119 set path=$ifaces/iface[. = "eth1:ha0"]/family value=inet
2013-04-13 15:45:02,731 DEBUG configure.models@set:119 set path=$ifaces/iface[. = "eth1:ha0"]/method value=static
2013-04-13 15:45:02,731 DEBUG configure.models@set:119 set path=$ifaces/iface[. = "eth1:ha0"]/netmask value=255.255.255.0
2013-04-13 15:45:02,731 DEBUG configure.models@set:119 set path=$ifaces/iface[. = "eth1:ha0"]/address value=10.90.90.50

In [4]: b.save()
---------------------------------------------------------------------------
IOError                                   Traceback (most recent call last)
<ipython-input-4-480966f9e668> in <module>()
----> 1 b.save()

/opt/solarsan/solarsan/configure/models.pyc in save(self)
    278
    279     def save(self):
--> 280         return self._aug.save()
    281
    282     @property

/opt/solarsan/_dev_lib/augeas.pyc in save(self)
    486         ret = Augeas._libaugeas.aug_save(self.__handle)
    487         if ret != 0:
--> 488             raise IOError("Unable to save to file!")
    489
    490     def load(self):

IOError: Unable to save to file!

In [5]: b._print('/augeas//error')
2013-04-13 15:45:09,846 DEBUG configure.models@get:115 get path=/augeas//error
2013-04-13 15:45:09,853 INFO configure.models@_print:137 [/augeas//error] = 'put_failed'
2013-04-13 15:45:09,853 DEBUG configure.models@match:123 match path=/augeas//error//*
2013-04-13 15:45:09,857 INFO configure.models@_print:140 [/augeas/files/etc/network/interfaces/error/path] = '/files/etc/network/interfaces'
2013-04-13 15:45:09,858 INFO configure.models@_print:140 [/augeas/files/etc/network/interfaces/error/lens] = '/usr/local/share/augeas/lenses/dist/interfaces.aug:101.13-.63:'
2013-04-13 15:45:09,858 INFO configure.models@_print:140 [/augeas/files/etc/network/interfaces/error/message] = 'Failed to match
    ({ /#comment/ = /[^\001-\004\t\n\r ][^\001-\004\n]*[^\001-\004\t\n\r ]|[^\001-\004\t\n\r ]/ } | { })*({ /iface/ = /[^\001-\004\t\n \\]+/ } | { /mapping/ = /[^\001-\004\t\n \\]+/ } | ({ /auto/ } | { /allow-aut((o[a-z-]|[a-np-z-])[a-z-]*|)|allow-au([a-su-z-][a-z-]*|)|(allow-a[a-tv-z-]|allow-[b-z-][a-z-])[a-z-]*|allow-a|allow-[b-z-]/ })({ /#comment/ = /[^\001-\004\t\n\r ][^\001-\004\n]*[^\001-\004\t\n\r ]|[^\001-\004\t\n\r ]/ } | { })*)*
  with tree
    {  } { "#comment" = "The loopback network iface" } { "auto" } { "iface" = "lo" } { "auto" } { "iface" = "eth1" } { "auto" } { "iface" = "eth0" } { "iface" } { "iface" } { "iface" } { "iface" }'

In [6]:

'''

class DebianInterfaceConfig(ReprMixIn, AugeasWrap):
    _file = '/etc/network/interfaces'
    _attrs = ['family', 'method', 'address', 'netmask', 'gateway', 'mtu']
    _map = {'dns-nameservers': 'nameservers',
            'dns-search': 'search'}
    _match = None
    _match_auto = None

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
        if value is True and not self.auto:
            self.set(str(self.name), '$ifaces/auto[last()+1]/1')
        elif value is False and self.auto:
            self.remove('$ifaces/auto/*[. = "%s"]' % str(self.name))

    @property
    def family(self):
        return self.get('%s/family' % self._match)

    @family.setter
    def family(self, value):
        return self.set(value, '%s/family' % self._match)

    @property
    def method(self):
        return self.get('%s/method' % self._match)

    @method.setter
    def method(self, value):
        return self.set(value, '%s/method' % self._match)

    @property
    def address(self):
        return self.get('%s/address' % self._match)

    @address.setter
    def address(self, value):
        return self.set(value, '%s/address' % self._match)

    @property
    def netmask(self):
        return self.get('%s/netmask' % self._match)

    @netmask.setter
    def netmask(self, value):
        return self.set(value, '%s/netmask' % self._match)

    @property
    def gateway(self):
        return self.get('%s/gateway' % self._match)

    @gateway.setter
    def gateway(self, value):
        return self.set(value, '%s/gateway' % self._match)

    @property
    def nameservers(self):
        return self.get('%s/nameservers' % self._match)

    @nameservers.setter
    def nameservers(self, value):
        return self.set(value, '%s/nameservers' % self._match)

    @property
    def search(self):
        return self.get('%s/search' % self._match)

    @search.setter
    def search(self, value):
        return self.set(value, '%s/search' % self._match)

    @property
    def mtu(self):
        return self.get('%s/mtu' % self._match)

    @mtu.setter
    def mtu(self, value):
        return self.set(value, '%s/mtu' % self._match)

    proto = method
    ipaddr = address

    def __init__(self, name_or_nic, replace=False):
        if isinstance(name_or_nic, Nic):
            nic = name_or_nic
        else:
            nic = Nic(name_or_nic)
        self.name = str(nic.name)

        super(DebianInterfaceConfig, self).__init__()
        self.load()

        if replace:
            logger.warning('Replace: removed %s', self.remove())
        #if replace and self.exists():
        #    logger.warning('Replacing existing interface config %s due to replace=%s', self, replace)
        #    self.remove()

        #if self.auto is None:
        #    self.auto = False
        #if self.family is None:
        #    self.family = 'inet'
        #if self.method is None:
        #    self.method = 'static'

    def exists_auto(self):
        return bool(self.get(self._match_auto))

    def load(self):
        self._aug.defvar('ifaces', '/files%s' % self._file)
        self._top_node = '$ifaces'
        self._match = '$ifaces/iface[. = "%s"]' % self.name
        self._match_auto = '$ifaces/auto/[* = "%s"]' % self.name

    def save(self):
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
        return convert_cidr_to_netmask(self._obj.get_netmask())

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
