
from solarsan import logging
log = logging.getLogger(__name__)
from solarsan.utils.stack import get_current_func_name
import struct  # for packing integers
import sys
from uuid import uuid4
from bunch import Bunch
import zmq
from ..encoders import pipeline
import datetime
from datetime import datetime
import time


#class KVMsg(Bunch):
class KVMsg(object):
    """
    Message is formatted on wire as 5 frames:
    frame 0: key (0MQ string)
    frame 1: sequence (8 bytes, network order)
    frame 2: uuid (blob, 16 bytes)
    frame 3: properties (0MQ string)
    frame 4: body (blob)
    """
    key = None
    sequence = 0
    uuid = None
    properties = None
    body = None

    _debug = False

    def __init__(self, sequence, uuid=None, key=None, properties=None, body=None):
        assert isinstance(sequence, int)
        self.sequence = sequence
        if uuid is None:
            uuid = uuid4().bytes
        self.uuid = uuid
        self.key = key
        self.properties = {} if properties is None else properties
        self.body = body
        self['created_at'] = datetime.now()

    # dictionary access maps to properties:
    def __getitem__(self, k):
        return self.properties[k]

    def __setitem__(self, k, v):
        self.properties[k] = v
        #if pipeline and pipeline in self.allowed_encoders:

    def get(self, k, default=None):
        return self.properties.get(k, default)

    def store(self, dikt):
        """Store me in a dict if I have anything to store
        else delete me from the dict."""
        if self.key is not None and self.body is not None:
            dikt[self.key] = self
            if self.get('ttl'):
                dikt[self.key]['ttl'] = time.time() + self['ttl']
        elif self.key in dikt:
            del dikt[self.key]

    def send(self, socket):
        """Send key-value message to socket; any empty frames are sent as such."""
        key = '' if self.key is None else self.key
        seq_s = struct.pack('!q', self.sequence)
        body = '' if self.body is None else self.body
        if body:
            body = pipeline.dump(body)
        #body_s = json.dumps(body)
        #prop_s = json.dumps(self.properties)
        prop_s = pipeline.dump(self.properties)
        socket.send_multipart([key, seq_s, self.uuid, prop_s, body])

    @classmethod
    def recv(cls, socket):
        """Reads key-value message from socket, returns new kvmsg instance."""
        return cls.from_msg(socket.recv_multipart())

    @classmethod
    def from_msg(cls, msg):
        """Construct key-value message from a multipart message"""
        if cls._debug:
            log.debug('msg=%s', msg)
        key, seq_s, uuid, prop_s, body = msg
        key = key if key else None
        seq = struct.unpack('!q', seq_s)[0]
        body = body if body else None
        if body:
            body = pipeline.load(body)
        #body = json.loads(body_s)
        #prop = json.loads(prop_s)
        prop = pipeline.load(prop_s)
        return cls(seq, uuid=uuid, key=key, properties=prop, body=body)

    def dump(self):
        if self.body is None:
            size = 0
            data = 'NULL'
        else:
            size = len(self.body)
            data = repr(self.body)
        return "[seq:{seq}][key:{key}][size:{size}] {props} {data}".format(
            seq=self.sequence,
            # uuid=hexlify(self.uuid),
            key=self.key,
            size=size,
            #props=json.dumps(self.properties),
            props=pipeline.dump(self.properties),
            data=data,
        )

    def __str__(self):
        return str(self.dump())

    #@property
    #def body(self):
    #    body = getattr(self, '_body', None)
    #    if not body:
    #        return

    #    #if pipeline and pipeline in self.allowed_encoders:
    #    #    body = self.allowed_encoders[serializer].load(body)
    #    #if pipeline:
    #    #    body = pipeline.load(body)

    #    return body

    #@body.setter
    #def body(self, value):
    #    #if pipeline:
    #    #    value = pipeline.dump(value)
    #    self._body = value

    #serializer = pipeline

    #@property
    #def serializer(self):
    #    return self.properties.get(get_current_func_name())

    #@serializer.setter
    #def serializer(self, value):
    #    self.properties[get_current_func_name()] = value

    def __repr__(self):
        data = {}
        for prop in ['key', 'properties', 'body', 'sequence']:
            v = getattr(self, prop, None)
            if v:
                data[prop] = v
        key = data.pop('key', None)
        body = data.pop('body', None)
        data[key] = body
        return '<%s %s>' % (self.__class__.__name__, data)

        # append = ''
        # for x, y in data.iteritems():
        #    append += ' %s=' % x + y
        # if append:
        #    append = append[1:]
        # return '<%s %s>' % (self.__class__.__name__, append)


# ---------------------------------------------------------------------
# Runs self test of class

def test_kvmsg(verbose):
    print " * kvmsg: ",

    # Prepare our context and sockets
    ctx = zmq.Context()
    output = ctx.socket(zmq.DEALER)
    output.bind("ipc://kvmsg_selftest.ipc")
    input = ctx.socket(zmq.DEALER)
    input.connect("ipc://kvmsg_selftest.ipc")

    kvmap = {}
    # Test send and receive of simple message
    kvmsg = KVMsg(1)
    kvmsg.key = "key"
    kvmsg.body = "body"
    if verbose:
        kvmsg.dump()
    kvmsg.send(output)
    kvmsg.store(kvmap)

    kvmsg2 = KVMsg.recv(input)
    if verbose:
        kvmsg2.dump()
    assert kvmsg2.key == "key"
    kvmsg2.store(kvmap)

    assert len(kvmap) == 1  # shouldn't be different

    # test send/recv with properties:
    kvmsg = KVMsg(2, key="key", body="body")
    kvmsg["prop1"] = "value1"
    kvmsg["prop2"] = "value2"
    kvmsg["prop3"] = "value3"
    assert kvmsg["prop1"] == "value1"
    if verbose:
        kvmsg.dump()
    kvmsg.send(output)
    kvmsg2 = KVMsg.recv(input)
    if verbose:
        kvmsg2.dump()
    # ensure properties were preserved
    assert kvmsg2.key == kvmsg.key
    assert kvmsg2.body == kvmsg.body
    assert kvmsg2.properties == kvmsg.properties
    assert kvmsg2["prop2"] == kvmsg["prop2"]

    print "OK"

if __name__ == '__main__':
    test_kvmsg('-v' in sys.argv)
