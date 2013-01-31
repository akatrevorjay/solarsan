
import mongoengine as m


class Target(m.Document):
    #meta = {'abstract': True}
    meta = {'allow_inheritance': True}

    name = m.StringField()
    luns = m.ListField()
    initiators = m.ListField()
    is_enabled = m.BooleanField()

    def enumerate_luns(self):
        return enumerate(self.luns)

    @property
    def is_enabled_int(self):
        return int(self.is_enabled)


class iSCSITarget(Target):
    #meta = {'allow_inheritance': True}
    pass


class SRPTarget(Target):
    #meta = {'allow_inheritance': True}
    pass
