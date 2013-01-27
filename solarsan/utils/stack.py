
import sys


# also, by calling sys._getframe(1), you can get this information
# for the *caller* of the current function.  So you can package
# this functionality up into your own handy functions:
def get_current_func_name():
    return sys._getframe(1).f_code.co_name
