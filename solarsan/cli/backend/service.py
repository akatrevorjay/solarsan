
from solarsan import logging
logger = logging.getLogger(__name__)
import rpyc
from . import AutomagicNode
from .root import CliRoot


class CLIService(rpyc.Service, AutomagicNode):
    def on_connect(self):
        logger.debug('Client connected.')

    def on_disconnect(self):
        logger.debug('Client disconnected.')

    #def ping(self):
    #    return True

    # Override the stupid prepending of expose_prefix to attrs, why is the
    # config not honored??
    def _rpyc_getattr(self, name):
        return getattr(self, name)

    #def _rpyc_delattr(self, name):
    #    pass

    #def _rpyc_setattr(self, name, value):
    #    pass

    """
    Nodes
    """

    def ui_child_cliroot(self):
        return CliRoot()
