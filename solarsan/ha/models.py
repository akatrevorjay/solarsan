

import mongoengine as m
from solarsan.models import CreatedModifiedDocMixIn, ReprMixIn
from cluster.models import Peer


class ActivePassiveIP(CreatedModifiedDocMixIn, ReprMixIn, m.Document):
    name = m.StringField(unique=True)
    ip = m.StringField()
    netmask = m.StringField()
    iface = m.StringField()
    peer = m.ReferenceField(Peer, dbref=False)
