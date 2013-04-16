
from solarsan import logging, signals
logger = logging.getLogger(__name__)


"""
SCST Log Policy
"""


@signals.check_log_entry.connect
def test_scst_log_policy(self, log):
    if not 'scst' in log.message:
        return
    logger.debug('SCST Log: %s', log.message)


"""
Target Log Policy
"""


class TargetLogPolicy(object):
    def __init__(self):
        pass

    def __call__(self, lw, log):
        pass


target_log_policy = TargetLogPolicy()
signals.check_log_entry.connect(target_log_policy)
