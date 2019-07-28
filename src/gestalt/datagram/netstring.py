from gestalt.datagram.endpoint import DatagramEndpoint
from gestalt.datagram.protocols.netstring import NetstringDatagramProtocol


class NetstringDatagramEndpoint(DatagramEndpoint):

    protocol_class = NetstringDatagramProtocol
