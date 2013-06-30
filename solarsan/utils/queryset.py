
#from solarsan.core import logger


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

            mod = None
            reverse = False
            attr = attr.split('__', 1)
            if len(attr) > 1:
                attr, mod = attr
                if mod.startswith('not'):
                    reverse = True
                    mod = mod.split('not', 1)[1]
            else:
                attr = attr[0]
            attr_val = getattr(arg, attr)

            #logger.debug("obj=%s, mod=%s, %s=%s, attr_vals=%s",
            #             arg, mod, attr, attr_val, attr_vals)

            if mod:
                if mod == 'lambda':
                    matched_vals = [v for v in attr_vals if not v(attr_val)]
                    if not reverse and matched_vals or reverse and len(matched_vals) != len(attr_vals):
                        add_arg = False
                        break

                elif mod in ['startswith', 'endswith']:
                    meth = getattr(attr_val, mod)
                    matched_vals = [v for v in attr_vals if not meth(v)]
                    if not reverse and matched_vals or reverse and len(matched_vals) != len(attr_vals):
                        add_arg = False
                        break
                else:
                    raise Exception("Unknown modifier '%s'" % mod)

            else:  # No modifier means simple == check
                if attr_val not in attr_vals:
                    add_arg = False
                    break

        #logger.debug("add_arg=%s", add_arg)

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

    def __init__(self, objs=None, base_filter=None, base_filter_replace=False, *args, **kwargs):
        if kwargs:
            if not base_filter:
                base_filter = dict()
            base_filter.update(kwargs)

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
            if args:
                self._objs = args
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
        append = ', '.join(['%s' % v for v in self._objs])
        return '<%s([%s])>' % (self.__class__.__name__, append)

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
