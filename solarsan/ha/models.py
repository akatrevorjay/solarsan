

import mongoengine as m
from solarsan.models import CreatedModifiedDocMixIn, ReprMixIn
from solarsan.cluster.models import Peer
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
    uuid = m.UUIDField()

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
