
from solarsan import LogMixin
import ejson


import uuid

@ejson.register_serializer(uuid.UUID)
def serialize_uuid(instance):
    return instance.urn

@ejson.register_deserializer(uuid.UUID)
def deserialize_uuid(data):
    return uuid.UUID(data)


from . import message

@ejson.register_serializer(message.Message)
def serialize_message(instance):
    return dict(instance)

@ejson.register_deserializer(message.Message)
def deserialize_message(data):
    return message.Message(data)


#from .managers import keyvalue

#@ejson.register_serializer(keyvalue.KeyValueStorage)
#def serialize_KeyValueStorage(instance):
#    return instance._export()

#@ejson.register_deserializer(keyvalue.KeyValueStorage)
#def deserialize_KeyValueStorage(data):
#    ret = keyvalue.KeyValueStorage()
#    ret._import(data)
#    return ret


class EJSONEncoder(LogMixin):
    def encode(self, node_uuid, message_type, parts):
        header = dict()

        header['type'] = message_type
        header['from'] = node_uuid

        plist = [header]

        if parts:
            plist.extend(parts)

        return [str(ejson.dumps(p)) for p in plist]

    def decode(self, jparts):
        if not jparts:
            return

        try:
            parts = [ejson.loads(j) for j in jparts]
        except ValueError:
            self.log.error('Could not decode invalid JSON: %s', jparts)
            return

        header = parts[0]
        parts = parts[1:]

        return header['from'], header['type'], parts
