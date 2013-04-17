
from solarsan import logging
logger = logging.getLogger(__name__)
from solarsan import conf
#from solarsan.template import quick_template
#from solarsan.storage.drbd import DrbdResource
#from solarsan.storage.volume import Volume
from .models import iSCSITarget, SRPTarget
import sh
#import random

#from . import scstadmin

import sysfs


class Node(sysfs.Node):
    pass

sys = Node()


scst = sys.kernel.scst_tgt


def has_driver(driver):
    return driver in scst.targets


def get_driver(driver):
    return getattr(scst.targets, driver, None)


def is_driver_enabled(driver):
    driver = get_driver(driver)
    if not driver:
        return
    return bool(driver.enabled)


def has_target(target, driver):
    driver = get_driver(driver)
    return target in driver


def get_target(target, driver):
    driver = get_driver(driver)
    return getattr(driver, target, None)


def is_target_enabled(target, driver):
    target = get_target(target, driver)
    if not target:
        return
    return bool(target.enabled)


def has_device(device):
    return device in scst.devices


def has_device_group(group):
    return group in scst.device_groups


def target_has_lun(target, driver, lun, group=None):
    target = get_target(target, driver)
    if group:
        target = getattr(target.ini_groups, group, None)
    if not target:
        return
    return str(lun) in target.luns


def target_has_ini_group(target, driver, group):
    target = get_target(target, driver)
    return group in target.ini_groups


def config(config_file, force=False):
    """Read and apply the specified configuration file.
    -config <file>"""
    args = []
    if force:
        args.append('-force')
    args.extend(['-noprompt', '-config', config_file])
    sh.scstadmin(*args)
    return True


"""
-check_config <file>
Verify the syntax of the specified configuration file.

-write_config <file>
Save the current configuration to the specified file.
"""


def clear_config(force=True):
    """Remove all configured devices, targets  that  do  not  correspond  to  a  physical  entity,  dynamic  target
    attributes,  initiator  groups, LUNs and dynamic driver attributes. Disable all targets that correspond to a
    physical entity and disable all target drivers. Note: static SCST core, target and target driver  attributes
    that  have  been  modified  are not reset to their default value unless the corresponding kernel modules are
    reloaded.
    -clear_config"""
    args = []
    if force:
        args.append('-force')
    args.extend(['-noprompt', '-clear_config'])
    return True


"""
-list_handler [<handler>]
If no device handler name has been specified, show the names of all device handlers supported  by  the  cur-
rently  loaded  kernel  modules.  If a device handler name has been specified, list the names of the devices
that use that device handler.

-list_device [<device>]
If no device name has been specified, show the names of all configured devices.  If a device name  has  been
specified, show all attributes of the specified device.

-list_driver [<driver>]
If no target driver name has been specified, list the names of all target drivers supported by the currently
loaded kernel modules. If a target driver name has been specified, show the names of all  targets  that  use
the specified target driver.

-list_target [<target>] [-driver <driver>]
If  no  target  driver  name  has  been specified, show all target names for all target drivers. If a target
driver name has been specified, show all configuration information for the specified target.  That  informa-
tion includes the assigned LUNs and information about all initiator groups associated with the target.

-list_group [<group>] [-driver <driver>] [-target <target>]
If either the target driver name or the target name has not been specified, show information about all known
target drivers, targets and initiator groups.  If a target driver name and target name have been  specified,
show configuration information for the specified initiator group.

-list_scst_attr
Show name and value of all SCST core attributes.

-list_hnd_attr <handler>
Show name and value of all attributes of the specified device handler, and also the names of all device cre-
ation attributes.

-list_dev_attr <device>
Show name and value of all attributes of the specified device.

-list_drv_attr <driver>
Show name and value of all attributes of the specified target driver.

-list_tgt_attr <target> -driver <driver>
Show name and value of all attributes of the specified target.

-list_grp_attr <group> -target <target> -driver <driver>
Show name and value of all attributes of the specified initiator group.

-list_lun_attr <lun> -driver <driver> -target <target> [-group <group>]
Show name and value of all attributes of the specified LUN. The LUN number either refers to a LUN associated
with a target or to a LUN associated with an initiator group of a target.

-list_sessions
Show all active sessions for all targets.

-list_dgrp [<dgrp>]
If  no device group name has been specified, show all defined ALUA device groups. If a device group name has
been specified, show configuration information for that device group only.

-list_tgrp [<tgrp>] -dev_group <dgrp>
If no ALUA target group name has been specified, list the target groups associated with the specified device
group. If a target group name has been specified, show configuration information for that target group.

-list_tgrp_attr <tgrp> -dev_group <dgrp>
Show a list with all ALUA attributes of the specified target group.

-list_ttgt_attr <tgt> -dev_group <dgrp> -tgt_group <tgrp>
Show a list with all ALUA attributes of the specified target.

-set_scst_attr -attributes <p=v,...>
Set the value of one or more SCST core attributes.

-set_hnd_attr <handler> -attributes <p=v,...>
Set the value of one or more device handler attributes.

-set_dev_attr <device> -attributes <p=v,...>
Set the value of one or more device attributes.

-set_drv_attr <driver> -attributes <p=v,...>
Set the value of one or more target driver attributes.

-set_tgt_attr <target> -driver <driver> -attributes <p=v,...>
Set the value of one or more target attributes.

-set_grp_attr <group> -driver <driver> -target <target> -attributes <p=v,...>
Set the value of one or more initiator group attributes.

-set_lun_attr <lun> -driver <driver> -target <target> [-group <group>] -attributes <p=v,...>
Set  the value of one or more LUN attributes. The LUN number either refers to a LUN associated with a target
or to a LUN associated with an initiator group of a target.

-add_drv_attr <driver> -attributes <p=v,...>
Add one or more new attributes to the specified target driver and set these to the specified  values.  Which
attribute  names  are  valid  depends on the affected target driver. Adding the same attribute several times
will cause multiple values to be defined for that attribute.

-add_tgt_attr <target> -driver <driver> -attributes <p=v,...>
Add one or more new attributes to the specified  target  and  set  these  to  the  specified  values.  Which
attribute  names  are  valid  depends on the involved target driver. Adding the same attribute several times
will cause multiple values to be defined for that attribute.

-rem_drv_attr <driver> -attributes <p=v,...>
Remove an (attribute, value) pair from the specified target driver.

-rem_tgt_attr <target> -driver <driver> -attributes <p=v,...>
Remove an (attribute, value) pair from the specified target.

-set_tgrp_attr <tgrp> -dev_group <dgrp> -attributes <p=v,...>
        Set one or more attributes of the specified ALUA target group.

-set_ttgt_attr <tgt> -dev_group <dgrp> -tgt_group <tgrp> -attributes <p=v,...>
        Set one or more attributes of the specified ALUA target.
"""


def open_dev(device, handler, **attributes):
    """Create a new SCST device using the specified device handler and attributes.
    -open_dev <device> -handler <handler> -attributes <p=v,...>"""
    args = ['-noprompt', '-open_dev', device, '-handler', handler]
    if attributes:
        attrs = []
        for k, v in attributes.iteritems():
            attrs.append('%s=%s' % (k, v))
        args.extend(['-attributes', ','.join(attrs)])
    sh.scstadmin(*args)


def resync_dev(device):
    """Update device size. SCST caches the size of devices controlled by the  vdisk_fileio  and  the  vdisk_blockio
    device  handlers. This command will not only cause SCST to update the cached device size but will also cause
    any logged in initiator to be notified about the capacity change event.
    -resync_dev <device>"""
    sh.scstadmin('-noprompt', '-resync_dev', device)


def close_dev(device, handler, force=True):
    """Remove the specified device from SCST.
    -close_dev <device> -handler <handler>"""
    args = []
    if force:
        args.append('-force')
    args.extend(['-noprompt', '-close_dev', device, '-handler', handler])
    sh.scstadmin(*args)


def add_target(target, driver):
    """Add a target to a target driver.
    -add_target <target> -driver <driver>"""
    sh.scstadmin('-noprompt', '-add_target', target, '-driver', driver)


def rem_target(target, driver):
    """Remove a target from a target driver.
    -rem_target <target> -driver <driver>"""
    sh.scstadmin('-noprompt', '-rem_target', target, '-driver', driver)


def add_group(group, driver, target):
    """Add an initiator group to the specified target.
    -add_group <group> -driver <driver> -target <target>"""
    sh.scstadmin('-noprompt', '-add_group', group, '-driver', driver, '-target', target)


def del_group(group, driver, target):
    """Remove an initiator group from the specified target.
    -rem_group <group> -driver <driver> -target <target>"""
    sh.scstadmin('-noprompt', '-rem_group', group, '-driver', driver, '-target', target)


def add_init(init, driver, target, group):
    """Add an initiator to an initiator group. <init> is either an explicit initiator name  or  an  initiator  name
    pattern. The wildcard characters '*', '?' and '!'  are supported.
    -add_init <init> -driver <driver> -target <target> -group <group>"""
    sh.scstadmin('-noprompt', '-add_init', init, '-driver', driver, '-target', target, '-group', group)


def rem_init(init, driver, target, group):
    """Remove an initiator name or initiator name pattern from an initiator group.
    -rem_init <user> -driver <driver> -target <target> -group <group>"""
    sh.scstadmin('-noprompt', '-rem_init', init, '-driver', driver, '-target', target, '-group', group)


def move_init(init, driver, target, group1, group2):
    """Move an initiator or initiator name pattern from one initiator group to another.
    -move_init <init> -driver <driver> -target <target> -group <group1> -to <group2>"""
    sh.scstadmin('-noprompt', '-move_init', init, '-driver', driver, '-target', target, '-group', group1, '-to', group2)


def clear_inits(driver, target, group):
    """Remove all initiators from an initiator group.
    -clear_inits -driver <driver> -target <target> -group <group>"""
    sh.scstadmin('-noprompt', '-clear_inits', '-driver', driver, '-target', target, '-group', group)


def add_lun(lun, driver, target, device, group=None, **attributes):
    """Add a LUN to a target or initiator group.
    -add_lun <lun> -driver <driver> -target <target> [-group <group>] -device <device> -attributes <p=v,...>"""
    args = ['-noprompt', '-add_lun', lun, '-driver', driver, '-target', target]
    if group:
        args.extend(['-group', group])
    args.extend(['-device', device])
    if attributes:
        attrs = []
        for k, v in attributes.iteritems():
            attrs.append('%s=%s' % (k, v))
        args.extend(['-attributes', ','.join(attrs)])
    sh.scstadmin(*args)


def rem_lun(lun, driver, target, group=None):
    """Remove a LUN from a target or initiator group.
    -rem_lun <lun> -driver <driver> -target <target> [-group <group>]"""
    args = ['-noprompt', '-rem_lun', lun, '-driver', driver, '-target', target]
    if group:
        args.extend(['-group', group])
    sh.scstadmin(*args)


def replace_lun(lun, driver, target, device, group=None, **attributes):
    """Replace the device associated with a LUN by another device.
    -replace_lun <lun> -driver <driver> -target <target> [-group <group>] -device <device> -attributes <p=v,...>"""
    args = ['-noprompt', '-replace_lun', lun, '-driver', driver, '-target', target]
    if group:
        args.extend(['-group', group])
    args.extend(['-device', device])
    if attributes:
        attrs = []
        for k, v in attributes.iteritems():
            attrs.append('%s=%s' % (k, v))
        args.extend(['-attributes', ','.join(attrs)])
    sh.scstadmin(*args)


def clear_luns(driver, target, group=None):
    """Remove all LUNs from a target or initiator group.
    -clear_luns -driver <driver> -target <target> [-group <group>]"""
    args = ['-noprompt', '-clear_luns', '-driver', driver, '-target', target]
    if group:
        args.extend(['-group', group])
    sh.scstadmin(*args)


def enable_target(target, driver):
    """Enable a target.
    -enable_target <target> -driver <driver>"""
    target = get_target(target, driver)
    target.enable = 1
    return True


def disable_target(target, driver):
    """Disable a target.
    -disable_target <target> -driver <driver>"""
    target = get_target(target, driver)
    target.enable = 0
    return True


def issue_lip(target=None, driver=None):
    """Issue  a LIP (Loop Initialization Protocol, fibre channel) for a specific target or for all drivers and tar-
    gets.
    -issue_lip [<target>] [-driver <driver>]"""
    args = ['-noprompt', '-issue_lip']
    if target:
        args.append(target)
    if driver:
        args.extend(['-driver', driver])
    sh.scstadmin(*args)





#class Scst(object):
#    root = SCST

#    def get_path(self, path):
#        if path.startswith(self.root):
#            return path
#        return os.path.join(self.root, path)

#    def isdir(self, path):
#        return os.path.isdir(self.get_path(path))

#    def isfile(self, path):
#        return os.path.isfile(self.get_path(path))

#    #def get(self, key):
#    #    is os.path.isdir(path):
#    #        return self.DIR

#    def get(self, path):
#        path = self.get_path(path)
#        if not os.path.isfile(path):
#            return
#        ret = open(path, 'rb').read()
#        return ret

#    def set(self, path, value):
#        path = self.get_path(path)
#        if not os.path.isfile(path):
#            return
#        pass

#    def walk(self, path):
#        pass

#    @property
#    def threads(self):
#        return self.get('threads').splitlines()[0]

#    @threads.setter
#    def threads(self, value):
#        value = int(value)
#        if value < 1 or value > 128:
#            raise ValueError('%s is not a valid option for threads' % value)
#        self.set('threads', value)


#class ScstTarget(Scst):
#    def __init__(self, driver, target):
#        self.driver = driver
#        self.target = target
#        self.root = get_target_path(driver, target)


#class ScstTargetGroup(ScstTarget):
#    def __init__(self, driver, target, group):
#        ScstTarget.__init__(self, driver, target)
#        self.group = group
#        self.root = get_target_ini_group_path(driver, target, group)


#class ScstDevice(object):
#    pass



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
