
import socket
hostname = socket.gethostname()


SOLARSAN_ROOT = '/opt/solarsan'
SERVER_ID = hostname


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


config = Config()


# Every box gets a UUID.
if not 'uuid' in config:
    from uuid import uuid1
    config['uuid'] = uuid1()
    config.save()


if not 'cluster_iface' in config:
    config['cluster_iface'] = 'eth1'
    config.save()


if not 'auto_snap' in config:
    config['auto_snap'] = {
        'daily': {
            'interval': 86400,
        },
        'hourly': {
            'interval': 3600,
        },
    }
    config.save()


# logging
LOGGING = {
    'version': 1,
    #'disable_existing_loggers': True,
    #'disable_existing_loggers': False,
    'root': {
        'level': 'DEBUG',
        'handlers': ['console', 'syslog'],
        #'level': 'WARNING',
        #'handlers': ['console', 'sentry'],
    },

    'formatters': {
        'standard': {
            'format': '%(asctime)s %(levelname)s %(name)s.%(module)s@%(funcName)s:%(lineno)d %(message)s',
            #'datefmt': '%d/%b/%Y %H:%M:%S',
        },
        'verbose': {
            'format': '%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s'
        },
        'syslog': {
            #'format': '<22>%(asctime)s ' + SERVER_NAME + ' %(name)s[%(process)d]: %(message)s',
            #'format': 'solarsan.%(name)s[%(process)d]: %(levelname)s %(message)s',
            #'format': 'solarsan/%(name)s.%(module)s/%(processName)s[%(process)d]: %(levelname)s %(message)s @%(funcName)s:%(lineno)d',
            'format': '%(name)s.%(module)s/%(processName)s[%(process)d]: %(message)s @%(funcName)s:%(lineno)d',
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
    },
    'loggers': {
        'django.db': {
            'propagate': True,
            #'level': 'DEBUG',
            'level': 'INFO',
        },
    }
}


rpyc_conn_config = {
    'allow_exposed_attrs': False,
    'allow_public_attrs': True,
    'allow_all_attrs': True,
    'allow_setattr': True,
    'allow_delattr': True,
    #'allow_pickle': False,
    #'exposed_prefix': '',
    'include_local_traceback': True,
    #'include_local_traceback': False,
}


scst_config_file = '/etc/scst.conf'
