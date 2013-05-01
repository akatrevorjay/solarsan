
from solarsan import logging, conf
logger = logging.getLogger(__name__)
from solarsan.conf import config
from solarsan.cli.backend import AutomagicNode
from solarsan.ha.models import FloatingIP
from solarsan.storage.volume import Volume
from solarsan.storage.drbd import DrbdResource
from .models import iSCSITarget, SRPTarget
from solarsan.target.models import DrbdResourceBackstore, VolumeBackstore


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


class PortalGroupNode(AutomagicNode):
    def __init__(self, group_index=None, group=None, target=None):
        self.target = target
        self.group_index = group_index
        self.group = group
        self.obj = self.group
        AutomagicNode.__init__(self)

    def ui_child_luns(self):
        return LunsNode(group=self.group, group_index=self.group_index, target=self.target)

    def ui_child_acl(self):
        return AclNode()


class LunsNode(AutomagicNode):
    def __init__(self, name=None, group=None, group_index=None, target=None):
        self.target = target
        self.group_index = group_index
        self.group = group
        self.obj = self.group
        AutomagicNode.__init__(self)

    def ui_children_factory_lun_dict(self):
        ret = {}
        for x, lun in enumerate(self.group.luns):
            ret[str(x)] = dict(args=(str(x), lun))
        return ret

    def ui_children_factory_lun(self, name, lun_index, lun):
        return LunNode(lun=lun, lun_index=lun_index, group=self.group, group_index=self.group_index, target=self.target)

    def ui_command_attach(self, volume=None, resource=None, index=None):
        assert not volume and resource

        backstore = None

        if resource:
            try:
                res = DrbdResource.objects.get(name=resource)
            except DrbdResource.DoesNotExist:
                raise ValueError('Resource "%s" does not exist.' % resource)

            backstore, created = DrbdResourceBackstore.objects.get_or_create(resource=res)
            if created:
                backstore.name = resource
                backstore.save()

        elif volume:
            vol = Volume(volume)
            if not vol.exists():
                raise ValueError('Volume "%s" does not exist.' % volume)

            # TODO Make sure this volume isn't being used by a resource

            backstore, created = VolumeBackstore.objects.get_or_create(volume_name=vol.name)
            if created:
                logger.info('Created Backstore "%s".', backstore)
                backstore.name = vol.basename()
                backstore.save()

        else:
            raise ValueError('You must specify either volume or resource to attach')

        if index is not None:
            self.group.luns.insert(index, backstore)
        else:
            self.group.luns.append(backstore)

        self.target.save()
        return True

    def ui_command_detach(self, lun):
        pass

    def ui_command_lsluns(self):
        return [lun for lun in self.group.luns]

    def ui_command_lsvolumes(self):
        return [lun for lun in self.group.luns if isinstance(lun, VolumeBackstore)]

    def ui_command_lsresources(self):
        return [lun for lun in self.group.luns if isinstance(lun, DrbdResourceBackstore)]


class LunNode(AutomagicNode):
    def __init__(self, lun, lun_index, group, group_index, target):
        self.target = target
        self.group = group
        self.group_inex = group_index
        self.obj = lun
        self.lun_index = lun_index

        AutomagicNode.__init__(self)


class AclNode(AutomagicNode):
    def ui_command_allow(self, initiator):
        pass

    def ui_command_deny(self, initiator):
        pass

    def ui_command_lsrecent(self):
        pass


class TargetNode(AutomagicNode):
    def ui_children_factory_portal_group_dict(self):
        ret = {}
        for x, group in enumerate(self.obj.groups):
            ret[group.name] = dict(args=(x, group))
        return ret

    def ui_children_factory_portal_group(self, name, group_index, group):
        return PortalGroupNode(group_index=group_index, group=group, target=self.obj)

    def ui_command_add_portal_group(self, name):
        pass

    def ui_command_lssessions(self):
        pass

    def __init__(self):
        super(TargetNode, self).__init__()

        self.define_config_group_param('target', 'name', 'string', 'Name', writable=False)
        self.define_config_group_param('target', 'uuid', 'string', 'UUID', writable=False)
        self.define_config_group_param('target', 'is_enabled', 'bool', 'Enabled in target subsystem', writable=False)

        self.define_config_group_param('target', 'floating_ip', 'string', 'Floating IP associated with this Target')

        for i in xrange(10):
            self.define_config_group_param('luns', '%s' % i, 'string', 'Lun %s device' % i)

        self.define_config_group_param('acl', 'allowed_initiators', 'string', 'Allowed initiators; space separated list')
        self.define_config_group_param('acl', 'denied_initiators', 'string', 'Denied initiators; space separated list')

        for i in xrange(10):
            self.define_config_group_param('acl', 'recent_initiator_%s' % i, 'string', 'Recent initiator %s' % i)

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
        luns = list(self.obj.get_all_luns())
        if key >= 0 and key < len(luns):
            ret = luns[key]
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

    def ui_getgroup_acl(self, key):
        pass

    def ui_setgroup_acl(self, key, value):
        pass

    def ui_command_save(self):
        self.obj.save()
        return True

    def summary(self):
        if self.obj.is_enabled:
            return ('active', True)
        elif self.obj.is_added:
            return ('added', True)
        else:
            return ('inactive', False)

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

        self.define_config_group_param('portal', 'address', 'string', 'Address:Port for iSCSI portal')

    def ui_getgroup_portal(self, key):
        pass

    def ui_setgroup_portal(self, key, value):
        pass


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

    def ui_getgroup_srpt(self, key):
        pass

    def ui_setgroup_srpt(self, key, value):
        pass


class SRPTargetNode(TargetNode):
    def __init__(self, name):
        self.obj = SRPTarget.objects.get(name=name)
        super(SRPTargetNode, self).__init__()
