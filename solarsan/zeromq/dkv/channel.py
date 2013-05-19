
from solarsan import logging
logger = logging.getLogger(__name__)
#from solarsan.pretty import pp
from solarsan.exceptions import DkvError, DkvTimeoutExceeded
#from .message import Message
#import zmq
from zmq.eventloop.zmqstream import ZMQStream


class ChannelError(DkvError):
    """Base for channel exceptions."""


class ChannelInUseError(ChannelError):
    """Raised if an already used channel is added to a ChannelProcessor."""


class Channel(object):
    """Mother of all channels. Defines the interface.

    Callbacks:
      The callbacks will receive the channel as first parameter and
      the message as second parameter. The error callback will get
      the stream where the error occured as second parameter.

    Attributes:
      * stream_in, stream_out : the streams for eventlopp handling
      * serializer : the serializer used
    """

    def __init__(self, socket_in, socket_out, serializer):
        self.stream_in = ZMQStream(socket_in)
        if socket_in == socket_out:
            self.stream_out = self.stream_in
        else:
            self.stream_out = ZMQStream(socket_out)
        self.serializer = serializer
        self._cb_receive = None
        self._cb_send = None
        self._cb_error = None
        self._chan_id = id(self)
        return

    def on_receive(self, callback):
        """Set callback to invoke when a message was received.
        """
        self.stream_in.stop_on_recv()
        self._cb_receive = callback
        if callback:
            self.stream_in.on_recv(self._on_recv)
        return

    def on_send(self, callback):
        """Set callback to invoke when a message was sent.
        """
        self.stream_out.stop_on_send()
        self._cb_send = callback
        if callback:
            self.stream_out.on_send(self._on_send)
        return

    def on_error(self, callback):
        """Set callback to invoke when an error event occured.
        """
        self.stream_in.stop_on_err()
        self.stream_out.stop_on_err()
        self._cb_error = callback
        if callback:
            self.stream_in.on_err(self._on_err_in)
            self.stream_out.on_err(self._on_err_out)
        return

    def send(self, message):
        """Send given message.
        """
        m = self.serializer.serialize(message)
        if self.serializer.multipart:
            self.stream_out.send_multipart(m)
        else:
            self.stream_out.send(m)
        return

    def _on_recv(self, msg):
        """Helper interfacing w/ streams.
        """
        if self.serializer.multipart:
            msg = self.serializer.deserialize(msg)
        else:
            msg = self.serializer.deserialize(msg[0])
        self._cb_receive(self, msg)
        return

    def _on_send(self, sent, _):
        """Helper interfacing w/ streams.
        """
        msg = sent[0]
        if self.serializer.multipart:
             msg = self.serializer.deserialize(msg)
        else:
            msg = self.serializer.deserialize(msg[0])
        self._cb_send(self, msg)
        return

    def _on_err_in(self):
        self._cb_error(self, self.stream_in)
        return

    def _on_err_out(self):
        self._cb_error(self, self.stream_out)
        return

    def __hash__(self):
        return self._chan_id

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self._chan_id == other._chan_id

    def __neq__(self, other):
        if not isinstance(other, self.__class__):
            return True
        return self._chan_id != other._chan_id
#

class ChannelProcessor(object):

    """Base class for channel handling.

    Subclasses need to overload the handle_* methods.
    """

    # map of channels -> (processor, (recv, error, send))
    _registered_channels = {}


    @classmethod
    def stop_all(cls):
        """Stop all processors.
        """
        for chan, (proc, flags) in cls._registered_channels.items():
            proc.stop()
        return

    def __init__(self):
        self._channels = []
        return

    def add_channel(self, channel, do_recv=True, do_error=True, do_send=False):
        """Registers this instance w/ given channel.
        """
        reg = self._registered_channels
        if channel in reg:
            raise ChannelInUseError(channel, reg[channel])
        flags = [False, False, False]
        if do_recv:
            channel.on_receive(self.handle_receive)
            flags[0] = True
        if do_error:
            channel.on_error(self.handle_error)
            flags[1] = True
        if do_send:
            channel.on_send(self.handle_send)
            flags[2] = True
        flags = tuple(flags)
        reg[channel] = (self, flags)
        self._channels.append((channel, flags))
        return

    def stop(self):
        """Remove all channels and callbacks. Stop working.
        """
        reg = self._registered_channels
        for chan, flags in self._channels:
            if flags[0]:
                chan.on_receive(None)
            if flags[1]:
                chan.on_error(None)
            if flags[2]:
                chan.on_send(None)
            reg.pop(chan, None)
        self._channels = []
        return

    def handle_receive(self, channel, message):
        raise NotImplementedError()

    def handle_send(self, channel, message):
        raise NotImplementedError()

    def handle_error(self, channel, stream):
        raise NotImplementedError()


class NodeChannel(object):
    '''
    Wraps a Node object with an interface that sends and receives over
    a specific channel
    '''
    def __init__(self, channel_name, node):
        self.channel_name = channel_name
        self.node = node

    @property
    def uuid(self):
        return self.node.uuid

    def create_subchannel(self, sub_channel_name):
        return Channel(self.channel_name + '.' + sub_channel_name, self.node)

    def add_message_handler(self, handler):
        return self.node.add_message_handler(self.channel_name, handler)

    def connect(self, *args, **kwargs):
        return self.node.connect(*args, **kwargs)

    def shutdown(self):
        return self.node.shutdown()

    def broadcast(self, message_type, *parts):
        return self.node.broadcast_message(
            self.channel_name, message_type, *parts)

    def unicast(self, to_uid, message_type, *parts):
        return self.node.unicast_message(
            to_uid, self.channel_name, message_type, *parts)
