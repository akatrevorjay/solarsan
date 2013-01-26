
"""
Exceptions
"""

import logging


class FormattedException(Exception):
    """Extends a normal Exception to support multiple *args used to format your message ala string formatting
    """
    def __init__(self, msg, *args, **kwargs):
        if msg and args:
            logging.exception(msg, *args)
            msg = msg % args

        # Call the base class constructor with the parameters it needs
        super(FormattedException, self).__init__(msg, **kwargs)


class LoggedException(FormattedException):
    """Logs an error when an exception occurs.
    Since it's baseclass is FormattedException it also supports formatting.
    """
    def __init__(self, *args, **kwargs):
        if args:
            logging.error(*args)
        super(LoggedException, self).__init__(*args, **kwargs)
