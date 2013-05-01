
from . import AutomagicNode
from solarsan.core.cli import SystemNode
from solarsan.storage.cli import Storage
from solarsan.target.cli import TargetsNode


"""
CLI Root
"""


class CliRoot(AutomagicNode):
    def __init__(self, *args, **kwargs):
        super(CliRoot, self).__init__(*args, **kwargs)

        self.define_config_group_param(
            'attribute', 'developer_mode', 'bool',
            'If true, enables developer mode.')

    """
    Nodes
    """

    def ui_child_system(self):
        return SystemNode()

    def ui_child_storage(self):
        return Storage()

    def ui_child_targets(self):
        return TargetsNode()

    """
    Old Ye Stuffe
    """

    def summary(self):
        return ('Thar be dragons.', False)
        return ('Ready.', True)

    #def refresh(self):
    #    for child in self.children:
    #        child.refresh()

    #def refresh(self):
    #    self.refresh()
