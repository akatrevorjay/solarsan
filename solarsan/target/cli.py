
from solarsan.cli.backend import AutomagicNode
from .models import iSCSITarget  # , SRPTarget


"""
Target
"""


class TargetsNode(AutomagicNode):
    def ui_child_iscsi(self):
        return iSCSITargetsNode()

    def ui_child_srp(self):
        return SRPTargetsNode()


class TargetNode(AutomagicNode):
    def summary(self):
        if self.obj.is_target_enabled:
            return ('Active', True)
        elif self.obj.is_target_added:
            return ('Added', True)
        else:
            return ('Inactive', False)


class iSCSITargetsNode(AutomagicNode):
    def ui_children_factory_iscsi_target_list(self):
        return [tgt.name for tgt in iSCSITarget.objects.all()]

    def ui_children_factory_iscsi_target(self, name):
        return iSCSITargetNode(name)

    def ui_command_create(self, wwn=None):
        raise NotImplemented


class iSCSITargetNode(TargetNode):
    def __init__(self, name):
        self.obj = iSCSITarget.objects.get(name=name)
        super(iSCSITargetNode, self).__init__()


class SRPTargetsNode(AutomagicNode):
    def ui_children_factory_srp_target_list(self):
        #return [tgt.name for tgt in SRPTarget.objects.all()]
        return []

    def ui_children_factory_srp_target(self, name):
        return SRPTargetNode(name)

    def ui_command_create(self, wwn=None):
        raise NotImplemented


class SRPTargetNode(TargetNode):
    def __init__(self, name):
        #self.obj = SRPTarget.objects.get(name=name)
        super(SRPTargetNode, self).__init__()
