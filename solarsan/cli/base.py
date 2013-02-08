
from configshell import ConfigNode
from cluster.models import Peer
from solarsan.pretty import pp
import sys


class BaseServiceConfigNode(ConfigNode):
    def __init__(self, name, parent):
        if not name:
            name = self.__class__.__name__.lower()
        super(BaseServiceConfigNode, self).__init__('system', parent)
        self._service = None

    @property
    def service(self):
        if not self._service:
            name = str(self.__class__.__name__).lower()
            p = Peer.get_local()
            self._service_cli = p.get_service('cli')
            meth = getattr(self._service_cli.root, name)
            self._service = meth()
        return self._service

    def __call__(self, *args, **kwargs):
        name = str(sys._getframe(1).f_code.co_name)
        name = name.partition('ui_command_')[2]
        meth = getattr(self.service, name)
        pp(meth(*args, **kwargs))
