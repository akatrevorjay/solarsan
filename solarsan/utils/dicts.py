"""
General Utils
Safe to run with.
"""

class LazyDict( dict ):
    def __getattr__( self, attr ):
        if attr in self:
            return self[attr]
        else:
            #return super(LazyDict,self).__getattr__(attr)
            raise AttributeError, "'%s' object has no attribute '%s'" \
                % ( self.__class__.__name__, attr )

    def __setattr__( self, attr, value ):
        if hasattr( super( LazyDict, self ), attr ):
            raise AttributeError, "'%s' object already has attribute '%s'" \
                % ( self.__class__.__name__, attr )
        self[attr] = value


class FilterableDict( dict ):
    """ Filter dict contents by str(key), list(keys), dict(key=value) """

    def filter( self, *args, **kwargs ):
        """ Filter dict object contents by str(key), list(keys), dict(key=value) where contents are an object and dict(key=value) are matched on obj.data """
        items = self.iteritems()
        for filter in list( list( *args ) + [dict( **kwargs )] ):
            if not len( filter ) > 0:
                continue
            filter_type = str( type( filter ) )
            if filter_type == "<type 'str'>":
                items = [( k, v ) for ( k, v ) in items if k == filter]
            elif filter_type == "<type 'list'>":
                items = [( k, v ) for ( k, v ) in items if k in filter]
            elif filter_type == "<type 'dict'>":
                items = [ ( k, v ) for ( k, v ) in items
                            for ( kwk, kwv ) in kwargs.items()
                              if v.get( kwk ) == kwv ]
        new = self.__new__( self.__class__, *list( items ) )
        return new


KEYNOTFOUND = '<KEYNOTFOUND>'       # KeyNotFound for dictDiff

def dict_diff( first, second ):
    """ Return a dict of keys that differ with another config object.  If a value is
        not found in one fo the configs, it will be represented by KEYNOTFOUND.
        @param first:   Fist dictionary to diff.
        @param second:  Second dicationary to diff.
        @return diff:   Dict of Key => (first.val, second.val)
    """
    diff = {}
    # Check all keys in first dict
    for key in first.keys():
        if ( not second.has_key( key ) ):
            diff[key] = ( first[key], KEYNOTFOUND )
        elif ( first[key] != second[key] ):
            diff[key] = ( first[key], second[key] )
    # Check all keys in second dict to find missing
    for key in second.keys():
        if ( not first.has_key( key ) ):
            diff[key] = ( KEYNOTFOUND, second[key] )
    return diff


def qdct_as_kwargs(qdct):
    kwargs = {}
    for k, v in qdct.items():
        kwargs[str(k)] = v
    return kwargs
