
#from solarsan.core import logger
#from solarsan import conf
from .base import BaseServiceConfigNode


class Benchmarks(BaseServiceConfigNode):
    def __init__(self, parent):
        super(Benchmarks, self).__init__('benchmarks', parent)

    def ui_command_netperf(self, host=None):
        self(host=host)

    def ui_command_cleanup(self, pool=None):
        self(pool=pool)

    def ui_command_bonniepp(self, atime='off', compress='on', pool=None):
        self(atime=atime, compress=compress, pool=pool)

    def ui_command_iozone(self, atime='off', compress='on', size='1M', pool=None):
        self(atime=atime, compress=compress, size=size, pool=pool)
