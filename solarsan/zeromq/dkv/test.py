
import gevent.monkey
gevent.monkey.patch_all()

from solarsan import logging, conf
logger = logging.getLogger(__name__)
logger = logging.LoggerAdapter(logger, {'context': None})
from solarsan.utils.files import slurp, burp

# from solarsan.zeromq.dkv import message, server, client, serializer,
# encoder, node, transaction
from solarsan.zeromq.dkv import node, message, transaction, encoder

import gevent
from bunch import Bunch
import sha
import jsonpickle



nodes_config = dict()

try:
    nodes_config = jsonpickle.decode(slurp('/tmp/nodes.json'))
except IOError as e:
    #raise e
     pass

nodes = dict()

bind_fmt = 'tcp://*:%s'
connect_fmt = 'tcp://127.0.0.1:%s'


def get_node_config(node):
    if not isinstance(node, basestring):
        return node
    global nodes_config
    if node not in nodes_config:
        x = len(nodes_config)
        uuid = sha.new(node).hexdigest()
        nodes_config[node] = Bunch(name=node, uuid=uuid, x=x, rtr=conf.ports.dkv_rtr + (
            5*x), pub=conf.ports.dkv_pub + (5*x))
        burp('/tmp/nodes.json', jsonpickle.encode(nodes_config))
        logger.info("Got node '%s' config: %s",
                    node, uuid, nodes_config[node])
    return nodes_config[node]


def get_node(name):
    if not isinstance(name, basestring):
        return name
    global nodes_config, nodes
    if not name in nodes:
        c = get_node_config(name)
        n = nodes[name] = node.Node(uuid=c.uuid)
        n._node_name = c.name
        logger.info('Got node %s', n)
    return nodes[name]


def get_node_stuffs(what):
    if isinstance(what, basestring):
        name = what
    else:
        name = what._node_name

    c = get_node_config(name)
    n = get_node(name)

    return (name, c, n)


def bind_node(what):
    name, c, n = get_node_stuffs(what)
    n.bind(bind_fmt % c.rtr, bind_fmt % c.pub)
    n.start()

def connect_node(n, to_n):
    (name, c, n) = get_node_stuffs(n)
    (to_name, to_c, to_n) = get_node_stuffs(to_n)
    to_uuid = to_n.uuid
    del to_n
    n.connect(to_uuid, connect_fmt % to_c.rtr, connect_fmt % to_c.pub)


def send_message(n):
    logger.info('Sending message')
    m = message.Message()
    m['omg'] = True
    t = transaction.Transaction(n, payload=m)
    t.start()
    del t


def main():
    na = get_node('a')
    nb = get_node('b')
    connect_node(na, nb)
    connect_node(nb, na)

    while True:
        gevent.sleep(1)
        # t.propose()
