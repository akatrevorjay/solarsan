#!/usr/bin/env python

import gevent.monkey
gevent.monkey.patch_all()

from solarsan import logging, conf, pp
logger = logging.getLogger(__name__)
logger = logging.LoggerAdapter(logger, {'context': None})
from solarsan.utils.files import slurp, burp

# from solarsan.zeromq.dkv import message, server, client, serializer,
# encoder, node, transaction
from solarsan.zeromq.dkv import node, message, transaction, encoder

import gevent
from solarsan.zeromq.dkv.base import _BaseDict as Bunch
import sha
import jsonpickle


nodes_config = dict()

try:
    nodes_config = jsonpickle.decode(slurp('/tmp/nodes.json'))
except IOError as e:
    # raise e
    pass

nodes = dict()

bind_fmt = 'tcp://*:%s'
connect_fmt = 'tcp://127.0.0.1:%s'


#NODE_LIST = set(['a', 'b', 'c'])
NODE_LIST = set(['a', 'b'])
logger.info('node_list=%s', NODE_LIST)


def get_node_config(node):
    if not isinstance(node, basestring):
        return node
    global nodes_config
    if node not in nodes_config:
        x = len(nodes_config)
        uuid = sha.new(node).hexdigest()
        nodes_config[node] = Bunch(name=node, uuid=uuid, x=x,
                                   rtr=conf.ports.dkv_rtr + (10*x), pub=conf.ports.dkv_pub + (10*x),
                                   backdoor=7000 + (10*x))
        burp('/tmp/nodes.json', jsonpickle.encode(nodes_config))
        logger.info("Got node '%s' config: %s",
                    node, uuid, dict(nodes_config[node]))
    return nodes_config[node]


def get_node(name):
    if not isinstance(name, basestring):
        return name
    global nodes_config, nodes
    if not name in nodes:
        c = get_node_config(name)
        n = nodes[name] = node.Node(uuid=c.uuid)
        n._node_name = c.name
        n.managers['Debugger'].backdoor_listen = '127.0.0.1:%s' % c.backdoor
        logger.info('Got node %s config: %s', n, dict(c))
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
    logger.debug('Binding node %s to %s', n, c)
    n.start()
    n.bind_listeners(bind_fmt % c.rtr, bind_fmt % c.pub)


def connect_nodes(n, *to_ns):
    for to_n in to_ns:
        (name, c, n) = get_node_stuffs(n)
        (to_name, to_c, to_n) = get_node_stuffs(to_n)
        to_uuid = to_n.uuid
        del to_n
        p = node.Peer(to_uuid)
        p.cluster_addr = '127.0.0.1'
        p.rtr_port = to_c.rtr
        p.pub_port = to_c.pub
        p.connect(n)


def send_message(n):
    logger.info('Sending message')
    m = message.Message()
    m['key'] = 'omg'
    m['value'] = True
    m['SHAFT!'] = "Hes a bad mother.. see BLAFT!"
    m['BLAFT!'] = 'SHUT YO MOUTH'
    t = transaction.Transaction(n, payload=m)
    t.start()
    del t


def main():
    ns = [get_node(name) for name in NODE_LIST]

    for n in ns:
        peer_list = NODE_LIST.difference(set([n._node_name]))
        bind_node(n)
        connect_nodes(n, *peer_list)

    for n in ns:
        n.wait_until_ready()

    while True:
        gevent.sleep(1)
        # t.propose()

if __name__ == '__main__':
    main()
