
try:
    import cPickle as pickle
except ImportError:
    import pickle


import zlib


class ZippedPickle:
    @staticmethod
    def dump(obj, protocol=-1):
        """pickle an object, and zip the pickle"""
        p = pickle.dumps(obj, protocol)
        return zlib.compress(p)

    @classmethod
    def send(cls, socket, obj, flags=0, protocol=-1):
        """pickle an object, and zip the pickle before sending it"""
        return socket.send(cls.dump(obj, protocol), flags=flags)

    @staticmethod
    def load(z):
        """Inverse of dump"""
        p = zlib.decompress(z)
        return pickle.loads(p)

    @classmethod
    def recv(cls, socket, flags=0):
        """inverse of send"""
        return cls.load(socket.recv(flags))


import blosc


class BloscPickle:
    @staticmethod
    def dump(obj, protocol=-1, typesize=254, clevel=9, shuffle=True):
        """pickle an object, and blosc the pickle"""
        p = pickle.dumps(obj, protocol)
        return blosc.compress(p, typesize, clevel, shuffle)

    @classmethod
    def send(cls, socket, obj, flags=0, protocol=-1):
        """pickle an object, and blosc the pickle before sending it"""
        return socket.send(cls.dump(obj, protocol), flags=flags)

    @staticmethod
    def load(z):
        """Inverse of dump"""
        p = blosc.decompress(z)
        return pickle.loads(p)

    @classmethod
    def recv(cls, socket, flags=0):
        """inverse of send"""
        return cls.load(socket.recv(flags))
