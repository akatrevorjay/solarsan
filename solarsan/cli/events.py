

class CliEvent(object):
    def __init__(self):
        super(CliEvent, self).__init__()

    def __call__(self):
        pass


class RawInputEvent(CliEvent):
    def __init__(self):
        super(RawInputEvent, self).__init__()

    def __call__(self):
        print "Woot"
        return raw_input('yes? ')
