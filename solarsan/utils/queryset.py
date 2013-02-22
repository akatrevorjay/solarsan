

def filter_by_attrs(args, **kwargs):
    """Takes a list of objects and returns only those where each **kwargs
    matches the attributes exactly.
    """
    if not kwargs:
        return args
    ret = []
    add_arg = True
    for arg in args:
        for attr, attr_vals in kwargs.items():
            if not isinstance(attr_vals, list):
                attr_vals = [attr_vals]

            #logging.debug("arg=%s getattr=%s attr=%s attr_vals=%s", arg,
            #              getattr(arg, attr), attr, attr_vals)
            if getattr(arg, attr) not in attr_vals:
                add_arg = False
                break
        #logging.debug("add_arg=%s", add_arg)
        if add_arg:
            ret.append(arg)
        else:
            add_arg = True
    return ret

    #return [arg for arg in args if
    #        all(
    #            [getattr(arg, k) == v for k, v in kwargs.items()]
    #        )]


class QuerySet(object):
    """QuerySet for object objs
    """
    _base_filter = None
    _lazy = None
    _objs = None

    def _get_objs(self):
        """This method can be overridden to specify how to get the list of obj if none are specified during inits"""
        return []

    def __init__(self, objs=None, base_filter=None, base_filter_replace=False):
        if base_filter:
            if base_filter_replace:
                self._base_filter = base_filter
            else:
                if not self._base_filter:
                    self._base_filter = {}
                self._base_filter.update(base_filter)
        if objs:
            self._objs = objs
        else:
            self._objs = self._get_objs()
        self._objs = list(self.filter())

    def all(self):
        return self._objs

    def filter(self, **kwargs):
        if self._base_filter:
            kwargs.update(self._base_filter)
        return filter_by_attrs(self, **kwargs)

    def __setitem__(self, k, v):
        self._objs[k] = v

    def append(self, v):
        self._objs.append(v)

    def __repr__(self):
        return '<%s(%s)>' % (self.__class__.__name__, self._objs.__repr__())

    def __len__(self):
        return len(self._objs)

    def __getitem__(self, key):
        return self._objs[key]

    def __delitem__(self, key):
        del self._objs[key]

    def __iter__(self):
        return iter(self._objs)

    def __reversed__(self):
        return reversed(self._objs)
