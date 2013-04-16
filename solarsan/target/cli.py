
from solarsan.conf import config
from solarsan.cli.backend import AutomagicNode
from solarsan.ha.models import FloatingIP
from solarsan.storage.volume import Volume
from solarsan.storage.drbd import DrbdResource
from .models import iSCSITarget, SRPTarget

"""
Target
"""


class TargetsNode(AutomagicNode):
    def __init__(self, *args, **kwargs):
        super(TargetsNode, self).__init__(*args, **kwargs)

        if not 'target' in config:
            config['target'] = {}
            config.save()
        #if not 'iscsi_enabled' in config['target']:
        #    config['target']['iscsi_enabled'] = True
        #    config.save()
        #if not 'srp_enabled' in config['target']:
        #    config['target']['srp_enabled'] = True
        #    config.save()

        #self.define_config_group_param('config', 'iscsi_enabled', 'bool',
        #                               'Enable iSCST Target Support')
        #self.define_config_group_param('config', 'srp_enabled', 'bool',
        #                               'Enable SRP Target Support')

    def ui_setgroup_config(self, k, v):
        config['target'][k] = v
        config.save()

    def ui_getgroup_config(self, k):
        return config['target'].get(k)

    def ui_child_iscsi(self):
        return iSCSITargetsNode()

    def ui_child_srp(self):
        return SRPTargetsNode()


class TargetNode(AutomagicNode):
    def __init__(self):
        super(TargetNode, self).__init__()

        self.define_config_group_param('target', 'name', 'string', 'Name', writable=False)
        self.define_config_group_param('target', 'uuid', 'string', 'UUID', writable=False)
        self.define_config_group_param('target', 'is_enabled', 'bool', 'Enabled in target subsystem', writable=False)

        self.define_config_group_param('target', 'floating_ip', 'string', 'Floating IP associated with this Target')

        self.define_config_group_param('luns', '0', 'string', 'Lun 0 device')
        self.define_config_group_param('luns', '1', 'string', 'Lun 1 device')
        self.define_config_group_param('luns', '2', 'string', 'Lun 2 device')
        self.define_config_group_param('luns', '3', 'string', 'Lun 3 device')
        self.define_config_group_param('luns', '3', 'string', 'Lun 3 device')
        self.define_config_group_param('luns', '4', 'string', 'Lun 4 device')
        self.define_config_group_param('luns', '5', 'string', 'Lun 5 device')

    def ui_getgroup_target(self, key):
        '''
        This is the backend method for getting keys.
        @param key: The key to get the value of.
        @type key: str
        @return: The key's value
        @rtype: arbitrary
        '''
        if key == 'floating_ip':
            fip = getattr(self.obj, key, None)
            if fip:
                return fip.name
        else:
            return getattr(self.obj, key, None)

    def ui_setgroup_target(self, key, value):
        '''
        This is the backend method for setting keys.
        @param key: The key to set the value of.
        @type key: str
        @param value: The key's value
        @type value: arbitrary
        '''
        if key == 'floating_ip':
            fip = FloatingIP.objects.get(name=value)
            self.obj.floating_ip = fip
        else:
            return setattr(self.obj, key, value)

    def ui_getgroup_luns(self, key):
        key = int(key)
        ret = None
        if key >= 0 and key < len(self.obj.devices):
            ret = self.obj.devices[key]
        if ret:
            return ret.name

    def ui_setgroup_luns(self, key, value):
        key = int(key)

        res = None
        vol = None

        try:
            res = DrbdResource.objects.get(name=value)
        except DrbdResource.DoesNotExist:
            vol = Volume(value)

        if res:
            self.obj.devices[key] = res
        elif vol.exists():
            self.obj.devices[key] = vol.name
        else:
            raise ValueError('Could not find replicated resource or Volume named "%s"' % value)

    def ui_command_save(self):
        self.obj.save()
        return True

    def summary(self):
        if self.obj.is_target_enabled:
            return ('Active', True)
        elif self.obj.is_target_added:
            return ('Added', True)
        else:
            return ('Inactive', False)

    def ui_command_delete(self):
        self.obj.delete()
        #self.refresh()
        return 'Deleted %s' % self.obj.name

    def ui_command_start(self):
        return self.obj.start()

    def ui_command_stop(self):
        return self.obj.stop()


class iSCSITargetsNode(AutomagicNode):
    def ui_children_factory_iscsi_target_list(self):
        return [tgt.name for tgt in iSCSITarget.objects.all()]

    def ui_children_factory_iscsi_target(self, name):
        return iSCSITargetNode(name)

    def ui_command_create(self):
        tgt = iSCSITarget()
        tgt.save()
        return 'Created %s' % tgt


class iSCSITargetNode(TargetNode):
    def __init__(self, name):
        self.obj = iSCSITarget.objects.get(name=name)
        super(iSCSITargetNode, self).__init__()


class SRPTargetsNode(AutomagicNode):
    def ui_children_factory_srp_target_list(self):
        return [tgt.name for tgt in SRPTarget.objects.all()]

    def ui_children_factory_srp_target(self, name):
        return SRPTargetNode(name)

    def ui_command_lsports(self):
        return [1, 2]

    def ui_command_create(self, name, port=1):
        # TODO port
        tgt = SRPTarget(name=name)
        tgt.save()
        return 'Created %s' % tgt


class SRPTargetNode(TargetNode):
    def __init__(self, name):
        self.obj = SRPTarget.objects.get(name=name)
        super(SRPTargetNode, self).__init__()
