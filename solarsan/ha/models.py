
from solarsan import logging, signals
logger = logging.getLogger(__name__)
import mongoengine as m
from solarsan.models import CreatedModifiedDocMixIn, ReprMixIn
from solarsan.cluster.models import Peer
from solarsan.configure.models import Nic, get_configured_ifaces
#from solarsan.utils.pings import ping_once
from uuid import uuid4


class FloatingIP(CreatedModifiedDocMixIn, ReprMixIn, m.Document):
    class signals:
        pre_start = signals.pre_start
        post_start = signals.post_start
        pre_stop = signals.pre_stop
        post_stop = signals.post_stop

    name = m.StringField(unique=True)
    peer = m.ReferenceField(Peer, dbref=False)
    uuid = m.UUIDField(binary=False)

    def save(self, *args, **kwargs):
        if not self.uuid:
            self.uuid = uuid4()
        if getattr(self, 'iface', None) and not self.interfaces:
            self.interfaces = [self.iface]
            self.iface = None
        super(FloatingIP, self).save(*args, **kwargs)

    @property
    def interfaces(self):
        for iface in get_configured_ifaces():
            if not iface.endswith(':%s' % self.name):
                continue
            yield iface

    @property
    def nics(self):
        for name in self.interfaces:
            yield Nic(name)

    def clean_iface_name(self, name):
        return '%s:%s' % (name.split(':', 1)[0], self.name)

    def add_interface(self, name, address):
        name = self.clean_iface_name(name)
        if name in self.interfaces:
            raise KeyError('%s already has an interface on %s' % (self, name))
        logger.info('Adding interface %s with address=%s to %s', name, address, self)
        is_active = self.is_active
        nic = Nic(name)
        nic.config.address = address
        nic.config.save()
        if is_active:
            nic.ifup()
        return True

    def remove_interface(self, name):
        name = self.clean_iface_name(name)
        if name not in self.interfaces:
            raise KeyError('%s does not have an interface on %s' % (self, name))
        logger.info('Removing interface %s from %s', name, self)
        is_active = self.is_active
        nic = Nic(name)
        if is_active:
            nic.ifdown()
        nic.config.remove()
        nic.config.save()
        return True

    @property
    def is_active(self):
        for nic in self.nics:
            if nic.is_ifup():
                return True
        return False

    #def _ping_ip(self):
    #    return ping_once(self.ip)

    def is_peer_active(self):
        storage = self.peer.get_service('storage', default=None)
        #if not storage:
        #    if self._ping_ip():
        #        return True
        #    else:
        #        return False
        return storage.root.floating_ip_is_active(self.name)

    def ifup(self, send_arp=True):
        logger.info('Floating IP "%s" is being brought up.', self)
        self.signals.pre_start.send(self)

        for nic in self.nics:
            nic.ifup(send_arp=send_arp)

        self.signals.post_start.send(self)

    def ifdown(self):
        logger.warning('Floating IP "%s" is being brought down.', self)
        self.signals.pre_stop.send(self)

        for nic in self.nics:
            nic.ifdown()

        self.signals.post_stop.send(self)

    def __unicode__(self):
        return self.name


#@signals.post_start.connect
#def _on_post_start(self):
#    if issubclass(self.__class__, FloatingIP):
#        logger.debug('self=%s', self)


#@signals.post_stop.connect
#def _on_post_stop(self):
#    if issubclass(self.__class__, FloatingIP):
#        logger.debug('self=%s', self)
