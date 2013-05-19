
from solarsan import logging  # signals, conf
logger = logging.getLogger(__name__)
from solarsan.exceptions import SolarSanError
from solarsan.pretty import pp
from datetime import datetime

# from .beacon_greeter import GreeterBeacon
# from ..encoders import pipeline
# from ..utils import get_address, parse_address
# from ..zhelpers import zpipe
# import zmq

import uuid

#class PickleableUUID(uuid.UUID):
#    def __setattr__(self,value):
#        return object.__setattr__(self, value)

from solarsan.utils.dates import timestamp_micro
from .transaction import Transaction
from .channel import Channel
from .node import Node


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


class _DictMessage(_BaseDict):

    def __init__(self, **kwargs):
        _BaseDict.__init__(self, **kwargs)
        if 'created_at' not in self:
            self.created_at = datetime.now()
        if 'sequence' not in self:
            self.sequence = None
        if self.proposed:
            self.proposed

    def propose(self):
        """Flood peers with proposal for us to get stored."""
        tx = self._tx = Transaction(message=self)
        return tx.propose(cb=self.on_propose)

    def cancel(self):
        tx = self._tx
        return tx.cancel(cb=self.on_cancel)

    def commit(self):
        tx = self._tx
        return tx.commit(cb=self.on_commit)

    def on_debug(self, *args, **kwargs):
        logger.debug('args=%s; kwargs=%s;', args, kwargs)

    on_propose = on_debug
    on_cancel = on_debug
    on_commit = on_debug


class MessageError(RuntimeError):

    """Base for Message errors.
    """


class InvalidDatatypeError(MessageError):

    """Raised when serialize encounters an invalid type.
    """


_allowed_types = (int, long, float, str, unicode, list, tuple, dict)
_skalars = (int, long, float, str, unicode)


def _uuid2hex(u):
    return b"UUID:" + u.hex


def _hex2uuid(u):
    return uuid.UUID(hex=u[5:])


def _codec_loop(fields, hdr, fnc_idx):
    """Helper to de-/encode a header dict.
    """
    ret = {}
    for field, codec in fields.items():
        try:
            v = hdr[field]
            if codec:
                fnc = codec[fnc_idx]
                if fnc:
                    v = fnc(v)
            ret[field] = v
        except KeyError:
            pass
    return ret


class MessageBase(object):

    """
    Base class for all Messages.

    Will set the basic header items:
      *id - message id (UUID)
      *ts - time stamp - (int, ms since epoch)
    """

    _base_hdr_fields = {'id': (_uuid2hex, _hex2uuid),
                        'ts': None,
                        }
    header_fields = {}

    @classmethod
    def from_parts(cls, header, payload=None):
        """Return cls instance initialized from header and payload.
        """
        header = cls._decode_header(header)
        ret = cls(header=header, payload=payload)
        return ret

    def __init__(self, header=None, payload=None):
        """Init instance.
        """
        self._header = header or {}
        self.payload = payload

        self.uuid
        self.ts

    @property
    def uuid(self):
        if not 'uuid' in self._header:
            self._header['uuid'] = uuid.uuid4()
        return self._header.get('uuid')

    @property
    def ts(self):
        if not 'ts' in self._header:
            self._header['ts'] = timestamp_micro()
        return self._header.get('ts')

    timestamp = ts

    @property
    def header(self):
        """Header as formated dict.
        """
        return self._encode_header()

    @property
    def raw_header(self):
        """Reference to header as untouched object.
        """
        return self._header

    @property
    def parts(self):
        """Tuple of header and payload.
        """
        return (self.header, self.payload)

    """
    Helpers
    """

    @classmethod
    def _decode_header(cls, h):
        """Decode header from dict h for cls.
        """
        fields = cls._base_hdr_fields.copy()
        fields.update(cls.header_fields)
        ret = _codec_loop(fields, h, 1)
        return ret

    def _encode_header(self):
        """Encode header.
        """
        fields = self._base_hdr_fields.copy()
        fields.update(self.header_fields)
        ret = _codec_loop(fields, self._header, 0)
        return ret

    def __repr__(self):
        fmt = b"%s.%s(%%r, %%r)" % (__name__, self.__class__.__name__)
        return fmt % self.parts

    def __eq__(self, o):
        """Messages are treated as equal if the uuids are equal.
        """
        return self.uuid.int == o.uuid.int

    def __neq__(self, o):
        return self.uuid.int != o.uuid.int

    def __hash__(self):
        return self.uuid.int


class Message(MessageBase):

    """
    Class adding type, sender and parent fields to header.

      * type is used to classify the message
      * sender is used to describe the origin of the message
      * parent is the uuid of the message that triggered the creation if this message
    """

    header_fields = {'type': None,
                     'sender': None,
                     'parent': (_uuid2hex, _hex2uuid),
                     }

    def __init__(self, type=None, sender=None, parent=None, **kwargs):
        super(Message, self).__init__(**kwargs)
        if type:
            self.type = type
        if sender:
            self.sender = sender
        if parent:
            self.parent = parent

    @property
    def sender(self):
        if 'sender' not in self._header:
            return None
        return self._header['sender']

    @sender.setter
    def sender(self, v):
        self._header['sender'] = v

    @property
    def type(self):
        return self._header.get('type')

    @type.setter
    def type(self, v):
        self._header['type'] = v

    @property
    def parent(self):
        return self._header.get('parent')

    @parent.setter
    def parent(self, v):
        if isinstance(v, MessageBase):
            v = v.uuid
        self._header['parent'] = v


class ChannelMessage(Message):

    """
    A Message with a channel.
    """

    header_fields = Message.header_fields.copy()
    header_fields.update(channel='')

    def __init__(self, channel='', **kwargs):
        super(ChannelMessage, self).__init__(**kwargs)
        self.channel = channel

    @property
    def channel(self):
        return self._header.get('channel')

    @channel.setter
    def channel(self, channel):
        self._header['channel'] = channel


class MessageContainer(list):

    """
    Container to hold your Messages
    """

    def append(self, object):
        return list.append(self, object)
