
""" Basics """

import socket

hostname = socket.gethostname()

SOLARSAN_ROOT = '/opt/solarsan'
#SERVER_ID = hostname

rpyc_conn_config = dict(
    allow_exposed_attrs=False,
    allow_public_attrs=True,
    allow_all_attrs=True,
    allow_setattr=True,
    allow_delattr=True,
    #allow_pickle=False,
    #exposed_prefix=,
    include_local_traceback=True,
    #include_local_traceback=False,
)

scst_config_file = '/etc/scst.conf'

ports = dict(
    discovery=1785,
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
    #'disable_existing_loggers': True,
    #'disable_existing_loggers': False,
    'root': {
        'level': 'DEBUG',
        #'handlers': ['console', 'syslog'],
        'handlers': ['console'],
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
            #'format': '%(asctime)s %(levelname)s %(name)s@%(funcName)s:%(lineno)d %(message)s',
            #'format': '%(asctime)s %(levelname)s %(name)s@%(funcName)s:%(lineno)d %(processName)s[%(process)d] {%(thread)d} %(message)s',
            #'format': '%(asctime)s %(levelname)8s %(funcName)20s:%(lineno)4d [%(process)6d] {%(thread)d} %(message)s',
            #'format': '>> %(asctime)s %(levelname)8s %(name)20s@%(funcName)20s:%(lineno)4d [%(process)6d] {%(thread)d}\n%(message)s\n',
            #'format': '%(asctime)s %(name)s/%(processName)s[%(process)d]: %(message)s @%(funcName)s:%(lineno)d',
            'format': '%(asctime)s %(name)s[%(process)d]: [%(levelname)s] %(message)s @%(funcName)s:%(lineno)d',
            #'format': '%(asctime)s %(name)s[%(process)d] {%(thread)d}: %(message)s @%(funcName)s:%(lineno)d',
        },
        'verbose': {
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
            'propagate': True,
            'level': 'DEBUG',
            'handlers': ['syslog'],
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
        #'solarsan.zeromq.clone': {
        #    'propagate': False,
        #    'level': 'INFO',
        #    'handlers': ['console'],
        #},
        'butler': {
            'propagate': False,
            'level': 'INFO',
            'handlers': ['console'],
        },
        'pizco': {
            'propagate': False,
            'level': 'INFO',
            'handlers': ['console'],
        },
    }
}
