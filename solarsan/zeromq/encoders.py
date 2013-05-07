
from solarsan import logging
logger = logging.getLogger(__name__)
from solarsan.utils.stack import get_last_func_name
from pqdict import PriorityQueueDictionary
#from bunch import Bunch

try:
    import cPickle as pickle
except ImportError:
    import pickle
import zmq.utils.jsonapi as json
import zlib


"""
Base
"""


class _SocketHelperMixIn:
    def send(self, socket, what, flags=0, *args, **kwargs):
        """Run pipeline on object, then send it down socket"""
        return socket.send(self.dump(what, *args, **kwargs), flags=flags)

    def recv(self, socket, flags=0):
        """inverse of send"""
        return self.load(socket.recv(flags))


class _Base(object, _SocketHelperMixIn):
    """Base for both encoders and compressors"""
    _remove_name_ending = None

    def __init__(self, **kwargs):
        for k, v in kwargs.iteritems():
            setattr(self, k, v)

    @property
    def name(self):
        name = str(self.__class__.__name__)
        if self._remove_name_ending:
            short = name.rsplit(self._remove_name_ending, 1)[0]
            if short:
                name = short
        return name

    def can_dump(self, what):
        """Returns bool if we can dump 'what'"""
        raise NotImplemented

    def can_load(self, what):
        """Returns bool if we can load 'what'"""
        raise NotImplemented

    def dump(self, what):
        raise NotImplemented

    def load(self, what):
        raise NotImplemented


class _Serializer(_Base):
    """Base serializer"""
    _remove_name_ending = 'Serializer'
    priority = 10


class _Compressor(_Base):
    """Base compressor"""
    _remove_name_ending = 'Compressor'
    priority = 20


class _NullMixIn:
    def dump(self, what):
        return what

    def load(self, what):
        return what


"""
Serializers
"""


class NullSerializer(_NullMixIn, _Serializer):
    """Null serializer"""


class PickleSerializer(_Serializer):
    """Pickle serializer"""
    protocol = -1

    def dump(self, what):
        """pickle an object, and zip the pickle"""
        return pickle.dumps(what, self.protocol)

    def load(self, what):
        """Inverse of dump"""
        return pickle.loads(what)


class JsonSerializer(_Serializer):
    def dump(self, what):
        return json.dumps(what)

    def load(self, what):
        return json.loads(what)


try:
    import msgpack
    from uuid import UUID

    class MsgPackSerializer(_Serializer):
        use_list = None

        def dump(self, what):
            return msgpack.packb(what, default=self._default)

        def _default(self, what):
            from .kvmsg import KVMsg

            #logger.debug('what=%s', what)
            cls = what.__class__.__name__
            state = getattr(what, '__dict__', None)
            if state:
                return (cls, state)
            return what

        def load(self, what):
            return msgpack.unpackb(what, object_hook=self._object_hook, list_hook=self._list_hook, use_list=self.use_list)

        def _object_hook(self, what):
            from .kvmsg import KVMsg

            #logger.debug('what=%s', what)
            return what

        def _list_hook(self, what):
            #logger.debug('what=%s', what)
            return what
except ImportError:
    pass


"""
Compressors
"""


class NullCompressor(_NullMixIn, _Compressor):
    """Null serializer"""


class ZippedCompressor(_Compressor):
    level = None
    window_size = None
    buffer_size = None

    def dump(self, what):
        args = []
        if self.level:
            args.append(self.level)
        args.append(what)
        return zlib.compress(*args)

    def load(self, what):
        args = []
        if self.window_size:
            args.append(self.window_size)
        if self.buffer_size:
            args.append(self.buffer_size)
        args.append(what)
        return zlib.decompress(*args)


try:
    import blosc

    class BloscCompressor(_Compressor):
        """Blosc compressor"""
        typesize = 254
        clevel = 9
        shuffle = True

        def dump(self, what):
            return blosc.compress(what, self.typesize, self.clevel, self.shuffle)

        def load(self, what):
            return blosc.decompress(what)
except ImportError:
    pass


"""
Pipeline
"""


class Pipeline(PriorityQueueDictionary, _SocketHelperMixIn):
    def add(self, cls_or_obj):
        if not isinstance(cls_or_obj, _Base):
            cls_or_obj = cls_or_obj()
        prio = cls_or_obj.priority
        super(Pipeline, self).add(cls_or_obj, prio)

    def __call__(self, cmd, what, check, *args, **kwargs):
        if cmd is None:
            cmd = get_last_func_name()

        keys = self.keys()
        if cmd == 'load':
            keys.reverse()

        for k in keys:
            meth = getattr(k, cmd, None)
            if meth:
                ret = meth(what, *args, **kwargs)
                if not check:
                    what = ret
                    continue
                elif not ret:
                    return ret

        if check:
            return True
        else:
            return what

    def dump(self, what):
        """Dumps 'what' into serialized format"""
        #if self._debug:
        #    logger.debug('pipeline predump=%s', what)
        what = self(None, what, False)
        #if self._debug:
        #    logger.debug('pipeline dump=%s', what)
        return what

    def load(self, what):
        """Loads 'what' from serialized format"""
        #if self._debug:
        #    logger.debug('pipeline preload=%s', what)
        what = self(None, what, False)
        #if self._debug:
        #    logger.debug('pipeline load=%s', what)
        return what


pipeline = Pipeline()
pipeline.add(PickleSerializer())
pipeline.add(ZippedCompressor())
