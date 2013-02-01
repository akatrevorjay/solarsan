
from solarsan.core import logger
from solarsan import conf
from solarsan.template import quick_template
from storage.drbd import DrbdResource
from .models import iSCSITarget
import sh


"""
Config
"""


def write_config():
    pri_ress = []
    pri_res_names = []
    pri_res_locals = {}
    for res in DrbdResource.objects.all():
        if res.local.service.is_primary:
            pri_ress.append(res)
            pri_res_names.append(res.name)
            pri_res_locals[res.name] = res.local

    iscsi_tgts = []
    devices = {}
    for tgt in iSCSITarget.objects.all():
        nope = None
        for lun in tgt.luns:
            if lun not in pri_res_names:
                logger.info('Target "%s" luns are not all available.', tgt.name)
                nope = True
                break
        if nope:
            for lun in tgt.luns:
                if lun in pri_res_names:
                    pri_res_locals[lun].service.secondary()
            continue
        logger.info('Target "%s" luns are available.', tgt.name)
        iscsi_tgts.append(tgt)

        for lun in tgt.luns:
            devices[lun] = pri_res_locals[lun]

    context = {
        'devices': devices,
        #'drbd_resources': pri_ress,
        'iscsi_targets': iscsi_tgts,
        #'srp_targets': SRPTarget.objects.all(),
    }
    quick_template('scst.conf', context=context, write_file=conf.scst_config_file)
    return True


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
