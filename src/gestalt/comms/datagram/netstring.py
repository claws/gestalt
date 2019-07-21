from gestalt.comms.datagram.endpoint import DatagramEndpoint
from gestalt.comms.datagram.protocols.netstring import NetstringDatagramProtocol


class NetstringDatagramEndpoint(DatagramEndpoint):

    protocol_class = NetstringDatagramProtocol
