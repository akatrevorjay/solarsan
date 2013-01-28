
#from ..core import logger
from .parsers.drbd import drbd_overview_parser


def drbd_find_free_minor():
    ov = drbd_overview_parser()
    minors = [int(x['minor']) for x in ov.values()]
    for minor in range(0, 9):
        if minor not in minors:
            return minor
    raise Exception("Could not find a free minor available")


DRBD_START_PORT = 7800


from socket import gethostname
from random import getrandbits
import mongoengine as m
from solarsan.template import quick_template
from storage.volume import Volume
from cluster.models import Peer


class DrbdPeer(m.EmbeddedDocument):
    peer = m.ReferenceField(Peer, dbref=False)
    minor = m.IntField()
    #pool = m.StringField()
    volume = m.StringField()

    @property
    def hostname(self):
        return self.peer.hostname

    @property
    def address(self):
        return self.peer.cluster_addr

    @property
    def port(self):
        return DRBD_START_PORT + self.minor

    def get_volume(self):
        return Volume(name=self.volume)

    @property
    def disk(self):
        return self.get_volume().device

    def __init__(self, **kwargs):
        super(DrbdPeer, self).__init__(**kwargs)

        #if not self.minor:
        #    self.minor = drbd_find_free_minor()

    @property
    def is_local(self):
        return self.hostname == gethostname()


class DrbdResource(m.Document):
    name = m.StringField(unique=True, required=True)
    shared_secret = m.StringField()
    #sync_rate = m.StringField()
    local = m.EmbeddedDocumentField(DrbdPeer, required=True)
    remote = m.EmbeddedDocumentField(DrbdPeer, required=True)
    #peers = m.ListField(m.ReferenceField(DrbdPeer, dbref=False))

    @property
    def peers(self):
        return [self.local, self.remote]

    def __init__(self, **kwargs):
        super(DrbdResource, self).__init__(**kwargs)

    def generate_random_secret(self):
        self.shared_secret = str(getrandbits(128))
        return True

    def write_config(self, confirm=False):
        out_file = None
        if confirm:
            out_file = '/etc/drbd.d/%s.res' % self.name

        context = {'res': self}
        return quick_template('drbd-resource.conf', context=context,
                              is_file=True, out_file=out_file)
