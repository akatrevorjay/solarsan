
from solarsan import logging  # signals, conf
logger = logging.getLogger(__name__)
#from solarsan.pretty import pp
#from solarsan.exceptions import SolarSanError

from datetime import datetime
#from .beacon_greeter import GreeterBeacon
#from ..encoders import pipeline
#from ..utils import get_address, parse_address
#from ..zhelpers import zpipe
#import zmq.green as zmq

#from solarsan.utils.dates import timestamp_micro
#from .transaction import Transaction
#from .channel import Channel
#from .node import Node


class _BaseDict(dict):
    def __init__(self, **kwargs):
        dict.__init__(self, **kwargs)

    def __repr__(self):
        return '<%s "%s">' % (self.__class__.__name__, dict.__repr__(self))

    def __setattr__(self, k, v):
        if k.startswith('_'):
            return object.__setattr__(self, k, v)
        return dict.__setitem__(self, k, v)

    def __getattr__(self, k):
        if k.startswith('_'):
            return object.__getattribute__(self, k)
        try:
            return dict.__getitem__(self, k)
        except KeyError:
            raise AttributeError


class Message(_BaseDict):
    def __init__(self, **kwargs):
        _BaseDict.__init__(self, **kwargs)
        if 'created_at' not in self:
            self.created_at = datetime.now()
        #if 'sequence' not in self:
        #    self.sequence = None


class MessageContainer(list):

    """
    Container to hold your Messages
    """

    def append(self, object):
        return list.append(self, object)
