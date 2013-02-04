#from solarsan.models import EnabledModelManager
#from jsonfield import JSONField
#from django.db import models
#from django.utils import timezone
#import datetime
#import logging

#from solarsan.core import logger
from solarsan import conf
from solarsan.models import CreatedModifiedDocMixIn, ReprMixIn
from solarsan.template import quick_template
import mongoengine as m

#import re
import ipcalc
import IPy
import netifaces
import pynetlinux
#from django.core.urlresolvers import reverse
import os


"""
Network
"""


def get_network_config():
    if not 'network' in conf.config:
        conf.config['network'] = {}
    return conf.config['network']


CIDR_CHOICES = (
    (1, '128.0.0.0'),
    (2, '192.0.0.0'),
    (3, '224.0.0.0'),
    (4, '240.0.0.0'),
    (5, '248.0.0.0'),
    (6, '252.0.0.0'),
    (7, '254.0.0.0'),
    (8, '255.0.0.0'),
    (9, '255.128.0.0'),
    (10, '255.192.0.0'),
    (11, '255.224.0.0'),
    (12, '255.240.0.0'),
    (13, '255.248.0.0'),
    (14, '255.252.0.0'),
    (15, '255.254.0.0'),
    (16, '255.255.0.0'),
    (17, '255.255.128.0'),
    (18, '255.255.192.0'),
    (19, '255.255.224.0'),
    (20, '255.255.240.0'),
    (21, '255.255.248.0'),
    (22, '255.255.252.0'),
    (23, '255.255.254.0'),
    (24, '255.255.255.0'),
    (25, '255.255.255.128'),
    (26, '255.255.255.192'),
    (27, '255.255.255.224'),
    (28, '255.255.255.240'),
    (29, '255.255.255.248'),
    (30, '255.255.255.252'),
    (31, '255.255.255.254'),
    (32, '255.255.255.255'),
)


def rev_dub_tup(iterator):
    iterator = dict(iterator)
    return dict(zip(iterator.values(), iterator.keys()))

NETMASK_CHOICES = rev_dub_tup(CIDR_CHOICES)


def convert_cidr_to_netmask(arg):
    return str(IPy.IP('0/%s' % arg, make_net=True).netmask())


def convert_netmask_to_cidr(arg):
    return int(IPy.IP('0/%s' % arg, make_net=True).prefixlen())


def get_interface(name):
    return pynetlinux.ifconfig.Interface(name)


class NicConfig(m.Document, CreatedModifiedDocMixIn):
    mac = m.StringField()
    name = m.StringField()
    PROTO_CHOICES = (
        ('none', 'Disabled'),
        ('static', 'Static IP'),
        ('dhcp', 'DHCP'),
    )
    proto = m.StringField()
    ipaddr = m.StringField()
    netmask = m.StringField()
    mtu = m.IntField()

    gateway = m.StringField()

    is_enabled = m.BooleanField()

    ##@property
    #def get_global_network_config(self):
    #    return get_network_config()

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

    def __init__(self, name=None):
        self.name = name
        # This has to be a basestring
        self._obj = get_interface(str(name))

    #@property
    #def _global_conf(self):
    #    return get_network_config()

    @property
    def config(self):
        if not hasattr(self, '_config'):
            self._config, created = NicConfig.objects.get_or_create(name=self.name)
        return self._config

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
        addrs = netifaces.ifaddresses(name)[netifaces.AF_INET]
        ret[name] = addrs
    return ret


def write_network_interfaces_config():
    """Write out network configuration"""
    context = dict(
        ifaces=NicConfig.objects.filter(is_enabled=True),
        netconf=get_network_config(),
    )
    return quick_template('configure/network/interfaces.template', context=context, is_file=True)
