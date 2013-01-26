
"""
Quick file funcs
"""


def slurp(fn):
    with open(fn, 'r') as fh:
        ret = fh.read()
    return ret


def burp(fn, data):
    with open(fn, 'w') as fh:
        fh.write(data)
