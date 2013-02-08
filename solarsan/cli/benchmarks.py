
from solarsan.core import logger
from solarsan import conf
from configshell import ConfigNode
import sh


class Benchmarks(ConfigNode):
    def __init__(self, parent):
        super(Benchmarks, self).__init__('benchmarks', parent)

    def ui_command_netperf(self, host=None):
        args = []
        if host:
            logger.info("Running client to host='%s'", host)
            args.extend(['-h', host])
        else:
            logger.info("Running server on 0.0.0.0")
        for line in sh.NPtcp(*args, _iter=True, _err_to_out=True):
            print line.rstrip("\n")

    test_pool = 'dpool'
    test_filesystem_name = '%(pool)s/omfg_test_benchmark'

    def _get_test_filesystem(self):
        pool = self.test_pool
        name = self.test_filesystem_name % {'pool': pool}
        try:
            fs = m.Filesystem.objects.get(name=name)
        except m.Filesystem.DoesNotExist:
            fs = m.Filesystem(name=name)
        return fs

    def _create_test_filesystem(self, atime='off', compress='on'):
        fs = self._get_test_filesystem()
        if fs.exists():
            logger.info("Destroying existing test filesystem '%s'", fs)
            fs.destroy(confirm=True)

        logger.info("Creating test filesystem '%s'", fs)
        fs.create()

        logger.info("Setting atime='%s' compress='%s'", atime, compress)
        fs.properties['atime'] = atime
        fs.properties['compress'] = compress

        logger.info("Changing ownership")
        sh.chown('nobody:nogroup', str(fs.properties['mountpoint']))

        return fs

    def _cleanup_test_filesystem(self, pool=None):
        if pool:
            self.test_pool = pool
        fs = self._get_test_filesystem()
        if not fs.exists():
            raise ZfsError("Could not destroy filesystem '%s' as it does not exist?", fs)
        logger.info("Destroying test filesystem '%s'", fs)
        fs.destroy(confirm=True)
        if fs.pk:
            fs.delete()

    def ui_command_cleanup(self, pool=None):
        if pool:
            self.test_pool = pool
        self._cleanup_test_filesystem()

    def ui_command_bonniepp(self, atime='off', compress='on', pool=None):
        if pool:
            self.test_pool = pool
        fs = self._create_test_filesystem(atime=atime, compress=compress)

        bonniepp = sh.Command('bonnie++')
        for line in bonniepp('-u', 'nobody', '-d', str(fs.properties['mountpoint']),
                             _iter=True, _err_to_out=True):
            print line.rstrip("\n")

        self._cleanup_test_filesystem()

    def ui_command_iozone(self, atime='off', compress='on', size='1M', pool=None):
        if pool:
            self.test_pool = pool
        fs = self._create_test_filesystem(atime=atime, compress=compress)

        cwd = os.curdir
        os.chdir(str(fs.properties['mountpoint']))

        try:
            with sh.sudo('-u', 'nobody', _with=True):
                for line in sh.iozone('-a', '-g', size, _iter=True, _err_to_out=True):
                    print line.rstrip("\n")
        finally:
            os.chdir(cwd)
        #os.chdir(cwd)
        logger.debug(os.curdir)
        time.sleep(1)

        self._cleanup_test_filesystem()


