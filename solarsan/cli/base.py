
from configshell import ConfigNode
from cluster.models import Peer
from solarsan.pretty import pp
import sys


class BaseServiceConfigNode(ConfigNode):
    """Black magic for a service based config node"""

    def __init__(self, name, parent):
        if not name:
            name = self.__class__.__name__.lower()
        super(BaseServiceConfigNode, self).__init__(name, parent)
        self._service = None

    @property
    def service(self):
        if not self._service:
            name = str(self.__class__.__name__).lower()
            p = Peer.get_local()
            self._service_cli = p.get_service('cli')
            meth = getattr(self._service_cli.root, name)

            args = []
            if hasattr(self, '_obj'):
                args.append(self._obj)

            self._service = meth(*args)
        return self._service

    def __call__(self, *args, **kwargs):
        frame = kwargs.pop('_frame', 1)
        ret_pp = kwargs.pop('_ret_pp', True)
        name = kwargs.pop('_meth', None)
        if not name:
            name = str(sys._getframe(frame).f_code.co_name)
            name = name.partition('ui_command_')[2]
        meth = getattr(self.service, name)
        ret = meth(*args, **kwargs)
        if ret_pp:
            pp(ret)
        else:
            return ret
