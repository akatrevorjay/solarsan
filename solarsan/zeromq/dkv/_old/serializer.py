from ..encoders import pipeline


class SerializerBase(object):
    """Base class for (de-)serializing.
    """
    #message_class = Message
    message_class = None

    pipeline = pipeline
    delimiter = b'\0||\0'
    multipart = False

    def __init__(self):
        if not self.message_class:
            from .message import Message
            self.__class__.message_class = Message

    def serialize(self, message):
        """Serialize the given message.

        message must be a MessageBase or derived from it.
        """
        header, payload = message.parts
        ret = self._serialize(header, payload)
        if not self.multipart:
            ret = self.delimiter.join(ret)
        return ret

    def deserialize(self, data):
        """Deserialize the raw data and return a message instance.
        """
        if not self.multipart:
            data = data.split(self.delimiter)
        header, payload = self._deserialize(data)
        msg = self.message_class.from_parts(header, payload)
        return msg

    def _serialize(self, header, payload):
        """Basic serialization.
        """
        ret = [self.pipeline.dump(header), self.pipeline.dump(payload)]
        return ret

    def _deserialize(self, data):
        """Basic derserialization.
        """
        h, p = data
        return (self.pipeline.loads(h), self.pipeline.loads(p))


class ChannelSerializer(SerializerBase):
    """Class for (de-)serializing messages with a channel header.

    The channel is put in front of the serialized data.
    """
    message_class = None

    def __init__(self):
        if not self.message_class:
            from .message import ChannelMessage
            self.__class__.message_class = ChannelMessage

    def _serialize(self, header, payload):
        """Serialization w/ channel prepended.
        """
        channel = header.pop('channel').encode('utf-8')
        ret = [channel, self.dumps(header), self.dumps(payload)]
        return ret

    def _deserialize(self, data):
        """Deserialization w/ channel in front.
        """
        t, h, p = data
        h, p = self.loads(h), self.loads(p)
        h[b'channel'] = t.decode('utf-8')
        return (h, p)
