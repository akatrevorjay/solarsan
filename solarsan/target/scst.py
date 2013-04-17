
from solarsan import logging
logger = logging.getLogger(__name__)
from solarsan import conf
from solarsan.template import quick_template
from solarsan.storage.drbd import DrbdResource
from solarsan.storage.volume import Volume
from .models import iSCSITarget, SRPTarget
import sh
import random


from . import scstadmin

SCST_SYS_PATH = scstadmin.SCST_SYS_PATH


class Scst(object):
    def __init__(self):
        pass


"""
Configuration
"""


def clear_config(force=True):
    args = []
    if force:
        args.append('-force')
    args.extend(['-noprompt', '-clear_config'])
    return True


def reload_config(force=False):
    args = []
    if force:
        args.append('-force')
    args.extend(['-noprompt', '-config', conf.scst_config_file])
    sh.scstadmin(*args)
    return True


"""
Init
"""


def status(self):
    try:
        sh.service('scst', 'status')
        return True
    except:
        return False


def start(self):
    return sh.service('scst', 'start')
