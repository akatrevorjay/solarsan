
from solarsan import logging
logger = logging.getLogger(__name__)
from solarsan import conf
from solarsan.template import quick_template
from solarsan.storage.drbd import DrbdResource
from solarsan.storage.volume import Volume
from .models import iSCSITarget, SRPTarget
import sh
import random


"""
Config
"""


class Device(object):
    name = None
    device = None
    t10_dev_id = None

    def __init__(self, volume=None, resource=None, backing_storage=None):
        if backing_storage:
            if isinstance(backing_storage, DrbdResource):
                resource = backing_storage
            elif isinstance(backing_storage, Volume):
                volume = backing_storage
        if resource:
            self.resource = resource

            self.name = resource.name
            self.device = resource.device
            self.t10_dev_id = resource.t10_dev_id
        elif volume:
            self.volume = volume

            self.name = volume.basename
            self.device = volume.device
            try:
                self.t10_dev_id = volume.properties['solarsan:t10_dev_id']
            except KeyError:
                self.t10_dev_id = volume.properties['solarsan:t10_dev_id'] = '0x%04d' % random.randint(0, 9999)
        else:
            raise Exception


def write_config(skip=None):
    ress = {}
    for res in DrbdResource.objects.all():
        if res.role == 'Primary' and skip != res.name:
            ress[res.name] = res

    iscsi_tgts = []
    devices = {}
    for tgt in iSCSITarget.objects.all():
        nope = None
        for lun in tgt.luns:
            if lun not in ress:
                logger.info('Target "%s" luns are not all available.', tgt.name)
                nope = True
                break
        if nope:
        #    for lun in tgt.luns:
        #        if lun in ress:
        #            ress[lun].local.service.secondary()
            continue
        logger.info('Target "%s" luns are available.', tgt.name)
        iscsi_tgts.append(tgt)

        for lun in tgt.luns:
            devices[lun] = Device(resource=ress[lun])

    context = {
        'devices': devices,
        #'drbd_resources': ress,
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
