#!/usr/bin/env python

from solarsan.core import logger

import Pyro4

from .server import StorageRPC


class PyroStorageRPC(StorageRPC):
    pass


def get_pyro_client(name):
    return Pyro4.Proxy("PYRONAME:%s" % name)


def run_server():
    logger.info("Starting Storage RPC Server..")
    storage_rpc = PyroStorageRPC()
    Pyro4.Daemon.serveSimple({
        storage_rpc: "solarsan.storage",
    }, ns=True)


def main():
    run_server()


if __name__ == '__main__':
    main()
