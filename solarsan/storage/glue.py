

_GET_TYPE_LOOKUP = {}


#from .dataset import Dataset
#Dataset._get_type_lookup.update(_GET_TYPE_LOOKUP)


def glue_get_type(cls, objtype):
    return _GET_TYPE_LOOKUP.get(objtype, cls)


from .pool import Pool
from .volume import Volume
from .snapshot import Snapshot


_GET_TYPE_LOOKUP.update(dict(
    pool=Pool,
    volume=Volume,
    snapshot=Snapshot,
))
