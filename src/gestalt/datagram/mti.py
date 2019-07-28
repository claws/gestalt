from gestalt.datagram.endpoint import DatagramEndpoint
from gestalt.datagram.protocols.mti import MtiDatagramProtocol


class MtiDatagramEndpoint(DatagramEndpoint):

    protocol_class = MtiDatagramProtocol
