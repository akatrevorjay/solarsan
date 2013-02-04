

import mongoengine as m
from solarsan.models import CreatedModifiedDocMixIn, ReprMixIn
from cluster.models import Peer
import sh
from netifaces import interfaces


class ActivePassiveIP(CreatedModifiedDocMixIn, ReprMixIn, m.Document):
    name = m.StringField(unique=True)
    ip = m.StringField()
    netmask = m.StringField()
    iface = m.StringField()
    peer = m.ReferenceField(Peer, dbref=False)

    @property
    def iface_name(self):
        return '%s:%s' % (self.iface, self.name)

    #def __init__(self, *args, **kwargs):
    #    super(ActivePassiveIP, self).__init__(*args, **kwargs)
    #    self.is_active = self.check_status()

    @property
    def is_active(self):
        return self.iface_name in interfaces()

    def ifup(self):
        sh.ifup(self.iface_name)

    def ifdown(self):
        sh.ifdown(self.iface_name)
