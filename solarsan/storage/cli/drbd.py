
#from solarsan.exceptions import ZfsError
from solarsan.cli.backend import AutomagicNode
from solarsan.storage.drbd import DrbdResource


class ResourcesNode(AutomagicNode):
    def ui_children_factory_resource_list(self):
        return [res.name for res in DrbdResource.objects.all()]

    def ui_children_factory_resource(self, name):
        return ResourceNode(name)

    def ui_command_create(self, name=None):
        # TODO Create Floating IP wizard
        raise NotImplemented


class ResourceNode(AutomagicNode):
    def __init__(self, name, display_name=None):
        if display_name:
            self.display_name = display_name
        self.obj = DrbdResource.objects.get(name=name)
        super(ResourceNode, self).__init__()

    def summary(self):
        return ('%s %s %s' % (self.obj.connection_state,
                              self.obj.role,
                              self.obj.disk_state,
                              ), True)
