
from solarsan import logging
logger = logging.getLogger(__name__)
#from solarsan.exceptions import ChannelError


class Channel(object):
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
