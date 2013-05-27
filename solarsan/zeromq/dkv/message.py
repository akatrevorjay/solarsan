
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
from uuid import uuid4
from solarsan.utils.uuids import get_uuid_datetime

from .base import _BaseDict


class Message(_BaseDict):
    def __init__(self, **kwargs):
        _BaseDict.__init__(self, **kwargs)
        if 'uuid' not in self:
            self.uuid = uuid4()

    @property
    def created_at(self):
        return get_uuid_datetime(self.uuid)


#class MessageContainer(list):
#
#    """
#    Container to hold your Messages
#    """
#
#    def append(self, object):
#        return list.append(self, object)
