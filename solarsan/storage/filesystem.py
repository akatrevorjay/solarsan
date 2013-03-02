
from solarsan.exceptions import ZfsError
import logging
import sh
from .dataset import Dataset


# TODO This is 100% broken
class _SnapshottableDatasetMixin(object):
    #def snapshots(self, **kwargs):
    #    """ Lists snapshots of this dataset """
    #    kwargs['type'] = 'snapshot'
    #    return self.children(**kwargs)

    #def filesystems(self, **kwargs):
    #    kwargs['type'] = 'filesystem'
    #    return self.children(**kwargs)

    #def snapshot(self, name, **kwargs):
    #    """ Create snapshot """
    #    zargs = ['snapshot']
    #    if kwargs.get('recursive', False) is True:
    #        zargs.append('-r')
    #    if not self.name:
    #        raise ZfsError("Snapshot was attempted with an empty name")
    #    #if kwargs.get('name_strftime', True) == True:
    #    #    name = timezone.now().strftime(name)
    #    if not self.exists():
    #        raise Exception("Snapshot was attempted on a non-existent dataset '%s'", self.name)
    #    name = '%s@%s' % (self.name, name)
    #    zargs.append(name)
    #
    #    logging.info('Creating snapshot %s with %s', name, kwargs)
    #    ret = iterpipes.check_call(cmd.zfs(*zargs))
    #    return Snapshot(name)
    pass


class Filesystem(Dataset):
    type = 'filesystem'

    def exists(self):
        """Checks if filesystem exists.

        filesystem = Filesystem('dpool/tmp/test0')
        filesystem.exists()

        """
        try:
            sh.zfs('list', '-t', 'filesystem', self.name)
        except sh.ErrorReturnCode_1:
            return False
        return True

    def create(self):
        """Creates storage filesystem.

        filesystem = Filesystem('dpool/tmp/test0')
        filesystem.create()

        """
        try:
            sh.zfs('create', self.name)
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
        """Destroys storage filesystem.

        filesystem = Filesystem('dpool/tmp/test0')
        filesystem.destroy()

        """
        if confirm is not True:
            raise ZfsError('Destroy of storage filesystem requires confirm=True')
        opts = ['destroy']
        if recursive:
            opts.append('-r')
        opts.append(self.name)
        sh.zfs(*opts)
        # TODO Force delete of this in bg (with '-d')
        return True

    def rename(self, new):
        """Renames storage filesystem.

        filesystem = Filesystem('dpool/r0')
        filesystem.rename('dpool/r1')

        """
        old = self.name
        logging.info("Renaming dataset '%s' to '%s'", old, new)
        try:
            sh.zfs('rename', old, new)
            self.name = new
        except:
            self.name = old
            raise
        finally:
            self.save()
