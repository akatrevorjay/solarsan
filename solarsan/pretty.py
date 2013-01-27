
from pprint import pformat
from pygments import highlight
from pygments.lexers import PythonLexer
#from pygments.lexers.web import JSONLexer
from pygments.formatters.terminal256 import Terminal256Formatter


def pp(arg):
    """Pretty prints with coloring for 256-color terms.
    Works in iPython, but not bpython as it does not write directly to term
    and decodes it instead."""
    print highlight(pformat(arg), PythonLexer(), Terminal256Formatter())
    #print highlight(pformat(arg), JSONLexer(), Terminal256Formatter())
