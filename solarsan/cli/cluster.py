
from .base import ServiceConfigNode


class Cluster(ServiceConfigNode):
    """Cluster"""

    def __init__(self, parent):
        super(Cluster, self).__init__(None, parent)
