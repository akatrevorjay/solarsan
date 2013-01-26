

def convert_bytes_to_human(n):
    """ Utility to convert bytes to human readable (K/M/G/etc) """
    SYMBOLS = ( 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y' )
    PREFIX = {}
    for i, j in enumerate( SYMBOLS ):
        PREFIX[j] = 1 << ( i + 1 ) * 10

    for i in reversed( SYMBOLS ):
        if n >= PREFIX[i]:
            value = float( n ) / PREFIX[i]
            return '%.1f%s' % ( value, i )


def convert_human_to_bytes(s):
    """ Utility to convert human readable (K/M/G/etc) to bytes """
    SYMBOLS = ( 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y' )
    PREFIX = {}
    for i, j in enumerate( SYMBOLS ):
        PREFIX[j] = 1 << ( i + 1 ) * 10

    s = str( s ).upper()
    for i in SYMBOLS:
        if s.endswith( i ):
            return '%.0f' % ( float( s[:-1] ) * PREFIX[i] )

    # Must be bytes or invalid data
    #TODO What if it's invalid data? How should that be handled?
    return '%.0f' % float( s )
