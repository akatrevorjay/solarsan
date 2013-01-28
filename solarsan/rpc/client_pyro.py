
import sys
import Pyro4
import Pyro4.util
sys.excepthook=Pyro4.util.excepthook

storage = Pyro4.Proxy('PYRONAME:solarsan.storage')

print storage.peer_ping()

