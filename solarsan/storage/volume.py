
from solarsan.utils import LoggedException
from solarsan.core import logger
import sh
from .dataset import Dataset


class Volume(Dataset):
    type = 'volume'

    @property
    def device(self):
        """Property: Volume device path"""
        # TODO This is Linux only.
        return '/dev/zvol/%s' % self.name

    def exists(self):
        """Checks if volume exists.

        volume = Volume('dpool/tmp/test0')
        volume.exists()

        """
        try:
            sh.zfs('list', '-t', 'volume', self.name)
        except sh.ErrorReturnCode_1:
            return False
        # TODO Force scan of this in bg
        return True

    def create(self, size, sparse=False, block_size=None, mkparent=False):
        """Creates storage volume.

        volume = Volume('dpool/tmp/test0')
        volume.create()

        """
        # TODO Check size to make sure it's decent.

        try:
            # -V volume, -s sparse, -b blocksize, -o prop=val
            # -p works like mkdir -p, creates non-existing parent datasets.
            opts = ['create']
            if sparse:
                opts.append('-s')
            if block_size:
                opts.extend(['-b', block_size])
            if mkparent:
                opts.append('-p')
            opts.extend(['-V', size, self.name])

            sh.zfs(*opts)
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
        """Destroys storage volume.

        volume = Volume('dpool/tmp/test0')
        volume.destroy()

        """
        if not confirm:
            raise LoggedException('Destroy of storage volume requires confirm=True')
        opts = ['destroy']
        if recursive:
            opts.append('-r')
        opts.append(self.name)
        sh.zfs(*opts)
        # TODO Force delete of this in bg
        return True

    def rename(self, new):
        """Renames storage volume.

        volume = Volume('dpool/r0')
        volume.rename('dpool/r1')

        """
        old = self.name
        logger.info("Renaming dataset '%s' to '%s'", old, new)
        try:
            sh.zfs('rename', old, new)
            self.name = new
        except:
            self.name = old
            raise
        finally:
            self.save()
