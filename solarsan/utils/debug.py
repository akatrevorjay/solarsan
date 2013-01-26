"""
Devel helpers
"""

import inspect


class TrueDebug(object):
    ### Alpha ###
    """A simple debugger. Add debug() to a function and it prints the function name and any objects included.
    Adding True to locale prints the file name where the function is. Adding False to log turns the log off.
    This feature can be modified to trace deeper and find the bugs faster, ending the puzzle box."""

    def __init__(self, objects=None, locale=False, log=True, parents=False):
        if log == False:
            return
        current = inspect.currentframe()
        if parents:
            self.get_parents(current)
        self.true_debug(current, objects, locale)

    def true_debug(self, current, objects, locale):
        debug_string = 'Function: ' + str(inspect.getouterframes(current)[1][3])
        #if locale == 'all': print inspect.getouterframes(current)[4]; return
        if objects is not None:
            debug_string += ' Objects: ' + str(objects)
        if locale:
            debug_string += ' File: ' + str(inspect.getouterframes(current)[1][1])
        print debug_string
        return

    def get_parents(self, current):
        debug_string = 'Function: ' + str(inspect.getouterframes(current)[1][3]) + ' Parents:'
        family = list(inspect.getouterframes(current))
        for parent in family:
            debug_string += ' ' + str(parent[4])
        print debug_string
        return

#debug = TrueDebug
