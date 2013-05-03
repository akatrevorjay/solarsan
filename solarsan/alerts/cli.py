
from solarsan.cli.backend import AutomagicNode


class AlertsNode(AutomagicNode):
    def ui_children_factory_email_list(self):
        return ['email:trevorj@localhostsolutions.com']

    def ui_children_factory_email(self, email):
        return EmailNode(email)


class EmailNode(AutomagicNode):
    def __init__(self, display_name=None):
        if not display_name:
            display_name = 'email:trevorj@localhostsolutions.com'
        self.display_name = display_name

        AutomagicNode.__init__(self)
