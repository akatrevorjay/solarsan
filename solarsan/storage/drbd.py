
from solarsan.core import logger
from solarsan import conf
from solarsan.template import quick_template
from cluster.models import Peer
from .volume import Volume
from .parsers.drbd import drbd_overview_parser
from random import getrandbits
import mongoengine as m
import sh


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
        return self.hostname == conf.hostname

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
    #peers = m.ListField(m.EmbeddedDocumentField(DrbdPeer))
    local = m.EmbeddedDocumentField(DrbdPeer, required=True)
    remote = m.EmbeddedDocumentField(DrbdPeer, required=True)
    shared_secret = m.StringField(required=True)
    sync_rate = m.StringField()

    # TODO Not needed, remove
    size = m.StringField(required=True)

    """
    Avoid possums
    """

    def __init__(self, **kwargs):
        super(DrbdResource, self).__init__(**kwargs)

    def __repr__(self):
        return "<%s '%s'>" % (self.__class__.__name__, self.name)

    def create_volumes(self, size):
        for peer in self.peers:
            peer.rpc('volume_create', peer.volume_full_name, size)

    def create_mds(self):
        for peer in self.peers:
            peer.rpc('drbd_res_create_md', self.name)

    def save(self, *args, **kwargs):
        # Automatically generate a random secret if one was not specified
        # already
        if not self.shared_secret:
            self.generate_random_secret()
        super(DrbdResource, self).save(*args, **kwargs)

    """
    Helpers to get which peer you really want
    """

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

    def promote_to_primary(self):
        # TODO Use rpyc
        sh.drbdadm('primary', self.name)

    """
    FUCK These are non-working for a sec, copied over from old Volume class, so need reworking
    """

    @property
    def replication_device_name(self):
        return self.path(-1)[0]

    @property
    def replication_device_path(self):
        return '/dev/drbd/by-res/%s' % self.replicated_device_name

    @property
    def replication_port(self):
        return int(DRBD_START_PORT) + int(self.replication_minor)

    # WIP TODO
    def replication_setup(self, is_source=False, peer=None, peer_hostname=None):
        if self.is_replicated:
            raise Exception('Volume "%s" is already clustered with Peer "%s"',
                            self, self.replication_peer.hostname)

        repl_name = self.replication_device_name

        # TODO Force a peer probe before moving forward just to make sure no
        # values ahev changed in the interim.
        # TODO Function that looks up peer by hostname and if it doesn't exist
        # in DB, it probes it before returning.
        if not peer and peer_hostname:
            #local = cm.Peer.objects.get(hostname=settings.SERVER_NAME)
            peer = Peer.objects.get(hostname=peer_hostname)

        logger.info('Setting up replicated Volume "%s" with Peer "%s"', self, peer)

        # Tell Peer to get ready
        #peer.rpc('cluster_volume_setup_as_peer', hostname=settings.SERVER_NAME)

        # Find free DRBD minor
        # TODO Write all configs in one go, it will make such things easier.
        minor = drbd_find_free_minor()

        # Set Volume props
        self.properties['solarsan:cluster_volume'] = 'pending'

        # Set DB props
        #self.is_replicated = True
        self.replication_peer = peer
        self.replication_minor = minor
        self.save()

        # Create Volume on Peer
        peer.rpc('volume_create',
                 size=self.properties['volsize'],
                 block_size=self.properties['volblock'])

        # Format volume for replication
        # Not needed with official tools init script?
        sh.drbdadm('create-md', repl_name)
        #local.rpc('cluster_volume_create_md', self.name)
        peer.rpc('cluster_volume_create_md', self.name)

        # CAREFUL If we're told to, forcefully overwrite peer's data.
        if is_source:
            repl_dev_path = self.replication_device_path
            sh.drbdsetup(repl_dev_path, 'primary', force='yes')

        # Set props indicating we're operating
        self.properties['solarsan:cluster_volume'] = 'true'
        #self.properties['solarsan:cluster_volume_peer_hostname'] = peer_hostname

        self.is_replicated = True
        self.save()

    def replication_promote_to_primary(self):
        repl_name = self.replication_device_name
        sh.drbdadm('primary', repl_name)

    def replication_demote_to_secondary(self):
        repl_name = self.replication_device_name
        sh.drbdadm('secondary', repl_name)

    def replication_write_config(self):
        pass

    """
    Replication Status
    """

    @property
    def replication_status(self):
        repl_name = self.replication_device_name
        return drbd_overview_parser(resource=repl_name)

    @property
    def replication_is_primary(self):
        status = self.replication_status
        role = status['role']
        if role == 'Primary':
            return True
        elif role == 'Secondary':
            return False

    @property
    def replication_is_connected(self):
        status = self.replication_status
        cstate = status['connection_state']

        if cstate == 'Connected':
            return True
        # TODO Better testing here
        else:
            return False

    @property
    def replication_is_up_to_date(self):
        status = self.replication_status
        dstate = status['disk_state']

        if dstate == 'UpToDate':
            return True
        else:
            return False

    @property
    def replication_is_healthy(self):
        status = self.replication_status
        cstate = status['connection_state']
        dstate = status['disk_state']

        if dstate == 'UpToDate' and cstate == 'Connected':
            return True
        else:
            return False
