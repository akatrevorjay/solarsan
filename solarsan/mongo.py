
import socket
PROJECT_NAME = 'solarsanweb'
SERVER_ID = socket.gethostname()


import mongoengine
MONGODB_DATABASES = {
    'default': {
        'name': "%s_%s" % (PROJECT_NAME, SERVER_ID),
    },
    'syslog': {
        'name': "syslog",
    },
}
def _register_mongo_databases():
    for k, v in MONGODB_DATABASES.items():
        v = v.copy()
        name = v.pop('name', k)
        mongoengine.register_connection(k, name, **v)
_register_mongo_databases()


