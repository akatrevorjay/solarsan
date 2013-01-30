
#from solarsan.models import Config
import socket


hostname = socket.gethostname()


rpyc_conn_config = {
    'allow_exposed_attrs': False,
    'allow_public_attrs': True,
    'allow_all_attrs': True,
    'allow_setattr': True,
    'allow_delattr': True,
    #'exposed_prefix': '',
}


#def get(name):
#    created, ret = Config.objects.get(name=name)
#    return ret
