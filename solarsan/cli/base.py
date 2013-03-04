
from solarsan.cluster.models import Peer
from solarsan.pretty import pp
from configshell import ConfigNode
import sys
import types
#import errno
#import time


def get_services_cli():
    return Peer.get_local().get_service('cli')


class ServiceConfigNode(ConfigNode):
    def __init__(self, name, parent, shell=None):
        if not name:
            name = self.__class__.__name__.lower()
        self._parent = parent

        self.generate_ui_commands()
        super(ServiceConfigNode, self).__init__(name, parent, shell=shell)
        self.generate_ui_children()
        self.generate_summary()

    def refresh(self):
        for k, v in self._generated_ui_commands.iteritems():
            delattr(self, k)
        self._generated_ui_commands.clear()
        self.generate_ui_commands()

        for k, v in self._generated_ui_children.iteritems():
            self.remove_child(v)
        self._generated_ui_children.clear()
        self.generate_ui_children()

        self.generate_summary()

        for child in self.children:
            child.refresh()

    def ui_command_refresh(self):
        self.refresh()

    """
    Generation
    """
    # TODO Generate attributes and parameters!

    def generate_ui_commands(self):
        self._generated_ui_commands = {}
        if not getattr(self.service, 'get_ui_commands', None):
            return
        for cmd_name, cmd in self.service.get_ui_commands().iteritems():
            self.generate_ui_command(cmd_name, cmd)

    def generate_ui_command(self, display_name, command):
        name = command.get('name', display_name)

        def func(self, *args, **kwargs):
            self(_meth=name, *args, **kwargs)
        func = types.MethodType(func, self)
        setattr(self, name, func)
        self._generated_ui_commands[name] = func

    def generate_ui_children(self):
        self._generated_ui_children = {}
        if not getattr(self.service, 'get_ui_children', None):
            return
        for child_name, child in self.service.get_ui_children().iteritems():
            self.generate_ui_child(child_name, child)

    def generate_ui_child(self, display_name, service_config):
        class child(ServiceConfigNode):
            _service_config = service_config
        child.__name__ = str(display_name)
        c = child(display_name, self)
        self._generated_ui_children[display_name] = c

    def generate_summary(self):
        if not getattr(self.service, 'summary', None):
            return

        def summary(self):
            return self.service.summary()
        summary = types.MethodType(summary, self)
        setattr(self, summary.__name__, summary)

    def ui_getgroup_property(self, property):
        '''
        This is the backend method for getting propertys.
        @param property: The property to get the value of.
        @type property: str
        @return: The property's value
        @rtype: arbitrary
        '''
        #return self(property)
        return self.service.ui_getgroup_property(property)

    def ui_setgroup_property(self, property, value):
        '''
        This is the backend method for setting propertys.
        @param property: The property to set the value of.
        @type property: str
        @param value: The property's value
        @type value: arbitrary
        '''
        #return self(property, value)
        return self.service.ui_setgroup_property(property, value)

    #def ui_getgroup_statistic(self, statistic):
    #    '''
    #    This is the backend method for getting statistics.
    #    @param statistic: The statistic to get the value of.
    #    @type statistic: str
    #    @return: The statistic's value
    #    @rtype: arbitrary
    #    '''
    #    if statistic in self.POOL_STATISTICS:
    #        obj = self._get_pool()
    #    else:
    #        obj = self._get_filesystem()
    #
    #    return str(obj.properties[statistic])

    #def ui_setgroup_statistic(self, statistic, value):
    #    '''
    #    This is the backend method for setting statistics.
    #    @param statistic: The statistic to set the value of.
    #    @type statistic: str
    #    @param value: The statistic's value
    #    @type value: arbitrary
    #    '''
    #    #self.obj.properties[statistic] = value
    #    return None

    """
    Service
    """

    _service = None
    _service_config = None

    @property
    def service(self):
        if not self._service:
            cls_name = str(self.__class__.__name__).lower()

            factory = None
            if self._service_config:
                factory = self._service_config.get('factory')

            if factory:
                name = factory
            else:
                name = 'ui_child_%s' % cls_name

            if getattr(self._parent, 'service', None):
                meth = getattr(self._parent.service, name)
            elif self.is_root():
                self._services_cli = cli = get_services_cli()
                meth = getattr(cli.root, name)
            else:
                raise Exception('No service found for "%s"' % cls_name)

            args = []
            if factory:
                real_name = None
                if self._service_config:
                    real_name = self._service_config.get('name')
                if not real_name:
                    real_name = cls_name
                args.append(real_name)
            #if hasattr(self, '_obj'):
            #    args.append(self._obj)

            self._service = meth(*args)
        return self._service

    def __call__(self, *args, **kwargs):
        frame = kwargs.pop('_frame', 1)
        ret_pp = kwargs.pop('_ret_pp', True)
        name = kwargs.pop('_meth', None)
        if not name:
            name = str(sys._getframe(frame).f_code.co_name)
        try:
            meth = getattr(self.service, name)
            ret = meth(*args, **kwargs)
        except Exception, e:
            ret = e.message

        if isinstance(ret, types.GeneratorType):
            for line in ret:
                print line

        elif isinstance(ret, basestring):
            ret = str(ret)

        elif isinstance(ret, (list, set, tuple)):
            ret = list(ret)

        elif isinstance(ret, dict):
            ret = dict(ret)

        elif isinstance(ret, bool):
            ret = bool(ret)

        elif isinstance(ret, int):
            ret = int(ret)

        if ret_pp:
            pp(ret)
        else:
            return ret
