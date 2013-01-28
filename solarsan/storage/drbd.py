
from solarsan.core import logger
from .parsers.drbd import drbd_overview_parser
from solarsan.template import quick_template
from .volume import Volume
#from ..cluster.models import Peer
from cluster.models import Peer

from socket import gethostname
from random import getrandbits
import mongoengine as m

#import zerorpc
#from ..rpc.client import StorageClient


def drbd_find_free_minor():
    ov = drbd_overview_parser()
    minors = [int(x['minor']) for x in ov.values()]
    for minor in range(0, 9):
        if minor not in minors:
            return minor
    raise Exception("Could not find a free minor available")


DRBD_START_PORT = 7800


class DrbdPeer(m.EmbeddedDocument):
    peer = m.ReferenceField(Peer, dbref=False, required=True)
    minor = m.IntField(required=True)
    pool = m.StringField(required=True)
    volume = m.StringField(required=True)

    def __init__(self, **kwargs):
        super(DrbdPeer, self).__init__(**kwargs)

    def save(self, *args, **kwargs):
        if not self.minor:
            #try:
            self.minor = self.peer.rpc('drbd_find_open_minor')
            #except (zerorpc.TimeoutExpired, zerorpc.LostRemote):
            #    pass
        ret = super(DrbdPeer, self).save(*args, **kwargs)
        return ret

    #@property
    #def rpc(self):
    #    if not getattr(self, '_rpc', None):
    #        # TODO migrate this stuff to Peer
    #        self._rpc = StorageClient(self.peer.cluster_addr)
    #    return self._rpc

    @property
    def is_local(self):
        """Checks if this DrbdPeer is localhost"""
        return self.hostname == gethostname()

    @property
    def hostname(self):
        return self.peer.hostname

    @property
    def address(self):
        return self.peer.cluster_addr

    @property
    def port(self):
        return DRBD_START_PORT + self.minor

    @property
    def volume_full_name(self):
        return '%s/%s' % (self.pool, self.volume)

    def get_volume(self):
        return Volume(name=self.volume_full_name)

    @property
    def disk(self):
        return self.get_volume().device


class DrbdResource(m.Document):
    # Volumes are made with this name; the Drbd resource is also named this.
    name = m.StringField(unique=True, required=True)
    size = m.StringField(required=True)
    peers = m.ListField(m.EmbeddedDocumentField(DrbdPeer))
    shared_secret = m.StringField(required=True)
    sync_rate = m.StringField()

    """
    Avoid possums
    """

    def __init__(self, **kwargs):
        super(DrbdResource, self).__init__(**kwargs)

    def create_volumes(self, size):
        for peer in self.peers:
            peer.rpc('volume_create', peer.volume_full_name, size)

    def create_mds(self):
        for peer in self.peers:
            peer.rpc('drbd_res_create_md', self.name)

    def save(self, *args, **kwargs):
        # There can only be two peers
        if len(self.peers) != 2:
            raise Exception("Wrong number of elements in self.peers. There should always be 2.")
        # Automatically generate a random secret if one was not specified
        # already
        if not self.shared_secret:
            self.generate_random_secret()
        super(DrbdResource, self).save(*args, **kwargs)

    """
    Helpers to get which peer you really want
    """

    @property
    def local(self):
        for peer in self.peers:
            if peer.is_local:
                return peer

    @property
    def remote(self):
        for peer in self.peers:
            if not peer.is_local:
                return peer

    """
    The others
    """

    def generate_random_secret(self):
        """Generates a random secret"""
        self.shared_secret = str(getrandbits(128))
        return True

    def write_config(self, confirm=False):
        """Generate a Drbd resource config file.
        Always returns generated config as str, with confirm=True it also
        writes it to disk."""
        out_file = None
        if confirm:
            out_file = '/etc/drbd.d/%s.res' % self.name

        context = {'res': self}
        return quick_template('drbd-resource.conf', context=context,
                              is_file=True, out_file=out_file)
