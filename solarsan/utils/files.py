
import os


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


"""
Gracefully lifted from rtslib.utils
"""


def fwrite(path, string):
    '''
    This function writes a string to a file, and takes care of
    opening it and closing it. If the file does not exist, it
    will be created.

    >>> from rtslib.utils import *
    >>> fwrite("/tmp/test", "hello")
    >>> fread("/tmp/test")
    'hello'

    @param path: The file to write to.
    @type path: string
    @param string: The string to write to the file.
    @type string: string

    '''
    path = os.path.realpath(str(path))
    file_fd = open(path, 'w')
    try:
        file_fd.write("%s" % string)
    finally:
        file_fd.close()


def fread(path):
    '''
    This function reads the contents of a file.
    It takes care of opening and closing it.

    >>> from rtslib.utils import *
    >>> fwrite("/tmp/test", "hello")
    >>> fread("/tmp/test")
    'hello'
    >>> fread("/tmp/notexistingfile") # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    IOError: [Errno 2] No such file or directory: '/tmp/notexistingfile'

    @param path: The path to the file to read from.
    @type path: string
    @return: A string containing the file's contents.

    '''
    path = os.path.realpath(str(path))
    string = ""
    file_fd = open(path, 'r')
    try:
        string = file_fd.read()
    finally:
        file_fd.close()

    return string
