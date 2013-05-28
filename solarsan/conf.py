
""" Basics """

from bunch import Bunch
import socket

hostname = socket.gethostname()

SOLARSAN_ROOT = '/opt/solarsan'
#SERVER_ID = hostname

rpyc_conn_config = Bunch(
    allow_exposed_attrs=False,
    allow_public_attrs=True,
    allow_all_attrs=True,
    allow_setattr=True,
    allow_delattr=True,
    allow_pickle=False,
    #exposed_prefix=,
    include_local_traceback=True,
    #include_local_traceback=False,
)

scst_config_file = '/etc/scst.conf'

ports = Bunch(
    bstar_primary=5003,
    bstar_secondary=5004,

    discovery=1785,

    dkv=5556,
    dkv_publisher=5557,
    dkv_collector=5558,

    dkv_rtr=6555,
    dkv_pub=6556,
    dkv_col=6557,

    #_rpc=1787,
    #_drbd_start=7800,
)

""" Config """

import os
from config import Config as _BaseConfig


CONFIG_FILE = os.path.join(SOLARSAN_ROOT, 'etc', 'solarsan', 'solarsan.conf')


class Config(_BaseConfig):
    def __init__(self):
        f = file(CONFIG_FILE, 'r')
        super(Config, self).__init__(f)

    def save(self):
        f = file(CONFIG_FILE, 'w')
        super(Config, self).save(f)


_config_updated = None
config = Config()


# Every box gets a UUID.
if not 'uuid' in config:
    from uuid import uuid1
    config['uuid'] = uuid1()
    _config_updated = True

if not 'cluster_iface' in config:
    config['cluster_iface'] = 'eth1'
    _config_updated = True

if not 'auto_snap' in config:
    config['auto_snap'] = {
        'daily': {
            'interval': 86400,
        },
        'hourly': {
            'interval': 3600,
        },
    }
    _config_updated = True

if _config_updated:
    del _config_updated
    config.save()

""" Logging """

LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    #'disable_existing_loggers': False,
    'root': {
        'level': 'DEBUG',
        #'handlers': ['console', 'syslog'],
        'handlers': ['console_color'],
        #'handlers': ['console_solarsan_color'],
        #'level': 'WARNING',
        #'handlers': ['console', 'sentry'],
    },

    'formatters': {
        #'standard': {
        #    'format': '%(asctime)s %(levelname)s %(name)s.%(module)s@%(funcName)s:%(lineno)d %(message)s',
        #    #'datefmt': '%d/%b/%Y %H:%M:%S',
        #},
        #'solarsan_standard': {
        'standard': {
            'datefmt': '%H:%M:%S',
            #'format': '%(asctime)s %(name)s[%(process)d]: [%(levelname)s] %(message)s @%(funcName)s:%(lineno)d',
            'format': '[%(asctime)s] {%(name)s} [%(levelname)s] %(message)s @%(funcName)s:%(lineno)d',
        },
        'color': {
            '()': 'colorlog.ColoredFormatter',
            'datefmt': '%H:%M:%S',
            #'format': "%(log_color)s%(levelname)-5s%(reset)s %(blue)s%(message)s"
            #'format': '%(yellow)s[%(reset)s%(asctime)s%(yellow)s] %(yellow)s{%(reset)s%(name)s%(yellow)s} [%(log_color)s%(levelname)s%(yellow)s] %(reset)s%(message)s %(bold)s%(purple)s@%(cyan)s%(funcName)s%(green)s:%(red)s%(lineno)d',
            'format': '%(bold)s[%(reset)s%(asctime)s%(bold)s] {%(reset)s%(name)s%(bold)s} [%(reset)s%(log_color)s%(levelname)s%(reset)s%(bold)s]%(reset)s %(message)s %(bold)s%(purple)s@%(cyan)s%(funcName)s%(green)s:%(red)s%(lineno)d',
        },
        'color_solarsan': {
            '()': 'colorlog.ColoredFormatter',
            'datefmt': '%H:%M:%S',
            #'format': "%(log_color)s%(levelname)-5s%(reset)s %(blue)s%(message)s"
            'format': '%(yellow)s[%(log_color)s%(asctime)s%(yellow)s] {%(log_color)s%(name)s%(yellow)s} [%(log_color)s%(levelname)s%(yellow)s] %(reset)s%(message)s %(bold)s%(purple)s@%(cyan)s%(funcName)s%(green)s:%(red)s%(lineno)d',
        },
        'std_solarsan': {
            'datefmt': '%H:%M:%S',
            #'format': '%(asctime)s %(levelname)s %(name)s@%(funcName)s:%(lineno)d %(message)s',
            #'format': '%(asctime)s %(levelname)s %(name)s@%(funcName)s:%(lineno)d %(processName)s[%(process)d] {%(thread)d} %(message)s',
            #'format': '%(asctime)s %(levelname)8s %(funcName)20s:%(lineno)4d [%(process)6d] {%(thread)d} %(message)s',
            #'format': '>> %(asctime)s %(levelname)8s %(name)20s@%(funcName)20s:%(lineno)4d [%(process)6d] {%(thread)d}\n%(message)s\n',
            #'format': '%(asctime)s %(name)s/%(processName)s[%(process)d]: %(message)s @%(funcName)s:%(lineno)d',
            #'format': '%(asctime)s %(name)s[%(process)d]: [%(levelname)s] %(message)s @%(funcName)s:%(lineno)d',
            #'format': '%(asctime)s %(name)s[%(process)d] {%(thread)d}: %(message)s @%(funcName)s:%(lineno)d',
            #'format': '%(asctime)s {%(name)s} %(levelname)s: %(message)s %(context)s @%(funcName)s:%(lineno)d',
            #'format': '%(asctime)s {%(name)-5s} %(levelname)-8s: %(message)s %(context)s @%(funcName)s:%(lineno)d',
            #'format': '%(date)s {%(name)-5s} %(levelname)-8s: %(message)s @%(funcName)s:%(lineno)d',
            'format': '[%(asctime)s] {%(name)s} [%(levelname)s] %(message)s @%(funcName)s:%(lineno)d',
        },
        'verbose': {
            'datefmt': '%H:%M:%S',
            #'format': '%(asctime)s %(levelname)s %(name)s@%(funcName)s:%(lineno)d %(processName)s[%(process)d] {%(thread)d} %(message)s',
            #'format': '%(asctime)s %(name)s/%(processName)s[%(process)d]: %(message)s {%(thread)d} @%(funcName)s:%(lineno)d',
            'format': '%(asctime)s %(name)s[%(process)d] {%(thread)d}: %(message)s @%(funcName)s:%(lineno)d',
        },
        'syslog': {
            #'format': '<22>%(asctime)s ' + SERVER_NAME + ' %(name)s[%(process)d]: %(message)s',
            #'format': 'solarsan.%(name)s[%(process)d]: %(levelname)s %(message)s',
            #'format': 'solarsan/%(name)s.%(module)s/%(processName)s[%(process)d]: %(levelname)s %(message)s @%(funcName)s:%(lineno)d',
            #'format': '%(name)s.%(module)s/%(processName)s[%(process)d]: %(message)s @%(funcName)s:%(lineno)d',
            'format': '%(name)s/%(processName)s[%(process)d]: %(message)s @%(funcName)s:%(lineno)d',
            #'celery_format': 'solarsan/%(name)s[%(process)d]: %(levelname)s %(message)s @%(funcName)s:%(lineno)d',
        },
    },
    'filters': {
        'require_debug_false': {
            '()': 'django.utils.log.RequireDebugFalse',
        },
        #'local_context': {
        #    '()': 'solarsan.logger.LocalContextFilter',
        #},
    },
    'handlers': {
        #'sentry': {
        #    'level': 'DEBUG',
        #    'class': 'raven.contrib.django.handlers.SentryHandler',
        #},
        #'mail_admins': {
        #    'level': 'ERROR',
        #    'class': 'django.utils.log.AdminEmailHandler',
        #},
        'null': {
            'level': 'DEBUG',
            'class': 'django.utils.log.NullHandler',
        },
        'console': {
            'level': 'DEBUG',
            #'class': 'logging.StreamHandler',
            'class': 'ConsoleHandler.ConsoleHandler',
            'formatter': 'standard',
        },
        'console_color': {
            'level': 'DEBUG',
            #'class': 'logging.StreamHandler',
            'class': 'ConsoleHandler.ConsoleHandler',
            'formatter': 'color',
        },
        'console_solarsan_color': {
            'level': 'DEBUG',
            #'class': 'logging.StreamHandler',
            'class': 'ConsoleHandler.ConsoleHandler',
            'formatter': 'color_solarsan',
        },
        'syslog': {
            'level': 'DEBUG',
            'class': 'logging.handlers.SysLogHandler',
            'formatter': 'syslog',
            'address': '/dev/log',
        },
        #'email': {
        #    'level': 'ERROR',
        #    'class': 'logging.handlers.SMTPHandler',
        #    'formatter': 'standard',
        #},
    },
    'loggers': {
        'django.db': {
            'propagate': True,
            #'level': 'DEBUG',
            'level': 'INFO',
        },
        'solarsan': {
            'level': 'DEBUG',
            'propagate': False,
            'handlers': ['console_solarsan_color'],
            #'filters': ['local_context'],
            #'propagate': True,
            #'handlers': ['syslog'],
        },
        'solarsan.rpc.server_storage': {
            'propagate': False,
            'level': 'DEBUG',
            'handlers': ['console'],
        },
        'solarsan.cli.backend': {
            'propagate': False,
            'level': 'DEBUG',
            'handlers': ['console'],
        },
        'solarsan.monitor.discovery': {
            'propagate': False,
            'level': 'INFO',
            #'level': 'DEBUG',
            'handlers': ['console'],
        },
        'solarsan.zeromq.beacon': {
            'propagate': False,
            'level': 'INFO',
            #'level': 'DEBUG',
            'handlers': ['console'],
        },
        #'solarsan.zeromq.dkv': {
        #    'propagate': False,
        #    'level': 'DEBUG',
        #    'handlers': ['console'],
        #},
        #'solarsan.zeromq.dkvsrv': {
        #    'propagate': False,
        #    'level': 'DEBUG',
        #    'handlers': ['console'],
        #},
        'butler': {
            'propagate': False,
            'level': 'INFO',
            'handlers': ['console'],
        },
        #'pizco': {
        #    'propagate': False,
        #    'level': 'INFO',
        #    'handlers': ['console'],
        #},
    }
}
