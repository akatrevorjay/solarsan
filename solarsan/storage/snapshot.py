
from solarsan.utils import LoggedException
#import logging
import sh
from .dataset import Dataset


class Snapshot(Dataset):
    type = 'snapshot'
    #TODO Check on __init__ if name contains '@' or not. It NEEDS to.

    def exists(self):
        """Checks if snapshot exists.

        snapshot = Snapshot('dpool/tmp/test0@whoa-snap-0')
        snapshot.exists()

        """
        try:
            sh.zfs('list', '-t', 'snapshot', self.name)
        except sh.ErrorReturnCode_1:
            return False
        # TODO Force scan of this in bg
        return True

    def create(self, size):
        """Creates storage snapshot.

        snapshot = Snapshot('dpool/tmp/test0@whoa-snap-0')
        snapshot.create()

        """
        # TODO Check size to make sure it's decent.

        try:
            sh.zfs('snapshot', self.name)
        except sh.ErrorReturnCode_1:
            # I'm not sure about this; the problem is if it creates the
            # dataset, but fails to mount it for some reason, we're left with
            # the pieces and a simple 1 retval...
            #if self.exists():
            #    self.destroy()
            raise
        # TODO Force scan of this in bg
        return True

    def destroy(self, confirm=False, recursive=False):
        """Destroys storage snapshot.

        snapshot = Snapshot('dpool/tmp/test0@whoa-snap-0')
        snapshot.destroy()

        """
        if not confirm:
            raise LoggedException('Destroy of storage snapshot requires confirm=True')
        opts = ['destroy']
        if recursive:
            opts.append('-r')
        opts.append(self.name)
        sh.zfs(*opts)
        # TODO Force delete of this in bg
        return True

    @property
    def snapshot_name(self):
        """ Returns the snapshot name """
        return self.basename.rsplit('@', 1)[1]

    @property
    def filesystem_name(self):
        """ Returns the associated filesystem/volume name """
        return self.basename.rsplit('@', 1)[0]

    #@property
    #def filesystem(self):
    #    """ Returns the associated filesystem for this snapshot """
    #    return Filesystem(self.filesystem_name)

    def clone(self, name, create_parent=False):
        """Clones snapshot into dataset

        snapshot = Snapshot('dpool/tmp/test0@whoa-snap-0')
        snapshot.clone('dpool/tmp/test1')

        """
        cmd = sh.zfs.bake('clone')
        opts = []
        if create_parent:
            opts.append('-p')
        opts.extend([self.name, name])
        cmd(*opts)
        return True
