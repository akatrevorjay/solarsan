
from solarsan import LogMixin


class DebugLogMixin(LogMixin):
    debug = False

    def _debug(self, *args, **kwargs):
        if self.debug:
            self.log.debug(*args, **kwargs)
