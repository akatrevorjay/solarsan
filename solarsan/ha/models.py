
from solarsan import logging
logger = logging.getLogger(__name__)
import mongoengine as m
from solarsan.models import CreatedModifiedDocMixIn, ReprMixIn
from solarsan.cluster.models import Peer
from solarsan.configure.models import DebianInterfaceConfig
import sh
from netifaces import interfaces
from solarsan.utils.pings import ping_once
from uuid import uuid4
from .arp import send_arp


class FloatingIP(CreatedModifiedDocMixIn, ReprMixIn, m.Document):
    name = m.StringField(unique=True)
    ip = m.StringField()
    netmask = m.StringField()
    iface = m.StringField()
    peer = m.ReferenceField(Peer, dbref=False)
    uuid = m.UUIDField(binary=False)

    def save(self, *args, **kwargs):
        if not self.uuid:
            self.uuid = uuid4()
        super(FloatingIP, self).save(*args, **kwargs)

    @property
    def iface_name(self):
        return '%s:%s' % (self.iface, self.name)

    #def __init__(self, *args, **kwargs):
    #    super(FloatingIP, self).__init__(*args, **kwargs)
    #    self.is_active = self.check_status()

    @property
    def is_active(self):
        return self.iface_name in interfaces()

    def _ping_ip(self):
        return ping_once(self.ip)

    def is_peer_active(self):
        storage = self.peer.get_service('storage', default=None)
        if not storage:
            if self._ping_ip():
                return True
            else:
                return False
        return storage.root.floating_ip_is_active(self.name)

    def ifup(self):
        if self.is_active:
            return
        sh.ifup(self.iface_name)
        send_arp(self.iface, self.ip)

    def ifdown(self):
        if not self.is_active:
            return
        sh.ifdown(self.iface_name)

    @classmethod
    def post_save(cls, sender, document, **kwargs):
        logger.debug('Post save: %s', document)
        logger.info('Creating interface config: %s', document)

        #debif = DebianInterfaceConfig(document.iface_name)
        #debif.remove()
        #debif._aug.save()
        #del debif

        debif = DebianInterfaceConfig(document.iface_name, replace=True)
        debif.family = 'inet'
        debif.method = 'static'
        debif.address = str(document.ip)
        debif.netmask = str(document.netmask)
        debif.save()

    @classmethod
    def pre_delete(cls, sender, document, **kwargs):
        logger.debug('Pre delete: %s', document)
        logger.info('Deleting interface config: %s', document)
        debif = DebianInterfaceConfig(document.iface_name)
        # hackery
        debif.remove()
        debif._aug.save()


m.signals.post_save.connect(FloatingIP.post_save, sender=FloatingIP)
m.signals.pre_delete.connect(FloatingIP.pre_delete, sender=FloatingIP)
