
from solarsan.core import logger
from solarsan import conf
from configshell import ConfigNode

from .system import System
from .developer import Developer
from .benchmarks import Benchmarks


class CliRoot(ConfigNode):
    def __init__(self, shell, sections):
        super(CliRoot, self).__init__('/', shell=shell)

        self.define_config_group_param(
            'global', 'developer_mode', 'bool',
            'If true, enables developer mode.')
        #print self.get_group_param('global', 'developer_mode')

        for section in sections:
            section(self)

        System(self)

        if conf.config.get('debug'):
            Developer(self)

    def summary(self):
        #return ('Thar be dragons.', False)
        return ('Ready.', True)

    def new_node(self, new_node):
        logger.info("New node: %s", new_node)
        return None

    def refresh(self):
        for child in self.children:
            child.refresh()

    def ui_command_refresh(self):
        self.refresh()

    #def ui_getgroup_global(self, key):
    #    '''
    #    This is the backend method for getting keys.
    #    @key key: The key to get the value of.
    #    @type key: str
    #    @return: The key's value
    #    @rtype: arbitrary
    #    '''
    #    logger.info("attr=%s", key)
    #    #return self.rtsnode.get_global(key)

    #def ui_setgroup_global(self, key, value):
    #    '''
    #    This is the backend method for setting keys.
    #    @key key: The key to set the value of.
    #    @type key: str
    #    @key value: The key's value
    #    @type value: arbitrary
    #    '''
    #    logger.info("attr=%s val=%s", key, value)
    #    #self.assert_root()
    #    #self.rtsnode.set_global(key, value)

    def ui_getgroup_param(self, param):
        '''
        This is the backend method for getting params.
        @param param: The param to get the value of.
        @type param: str
        @return: The param's value
        @rtype: arbitrary
        '''
        logger.info("attr=%s", param)
        #return self.rtsnode.get_param(param)

    def ui_setgroup_param(self, param, value):
        '''
        This is the backend method for setting params.
        @param param: The param to set the value of.
        @type param: str
        @param value: The param's value
        @type value: arbitrary
        '''
        logger.info("attr=%s val=%s", param, value)
        #self.assert_root()
        #self.rtsnode.set_param(param, value)

    def ui_getgroup_attribute(self, attribute):
        '''
        This is the backend method for getting attributes.
        @param attribute: The attribute to get the value of.
        @type attribute: str
        @return: The attribute's value
        @rtype: arbitrary
        '''
        logger.info("attr=%s", attribute)
        #return self.rtsnode.get_attribute(attribute)

    def ui_setgroup_attribute(self, attribute, value):
        '''
        This is the backend method for setting attributes.
        @param attribute: The attribute to set the value of.
        @type attribute: str
        @param value: The attribute's value
        @type value: arbitrary
        '''
        logger.info("attr=%s val=%s", attribute, value)
        #self.assert_root()
        #self.rtsnode.set_attribute(attribute, value)

    def ui_getgroup_parameter(self, parameter):
        '''
        This is the backend method for getting parameters.
        @param parameter: The parameter to get the value of.
        @type parameter: str
        @return: The parameter's value
        @rtype: arbitrary
        '''
        logger.info("parameter=%s", parameter)
        #return self.rtsnode.get_parameter(parameter)

    def ui_setgroup_parameter(self, parameter, value):
        '''
        This is the backend method for setting parameters.
        @param parameter: The parameter to set the value of.
        @type parameter: str
        @param value: The parameter's value
        @type value: arbitrary
        '''
        logger.info("parameter=%s val=%s", parameter, value)
        #self.assert_root()
        #self.rtsnode.set_parameter(parameter, value)


def main():
    from configshell.shell import ConfigShell

    #CLI_TOP_SECTIONS = [Storage, Logs, Configure, Cluster]
    CLI_TOP_SECTIONS = []

    shell = ConfigShell('~/.solarsancli')
    root_node = CliRoot(shell, CLI_TOP_SECTIONS)
    shell.run_interactive()

if __name__ == "__main__":
    main()
