from gestalt.comms.datagram.endpoint import DatagramEndpoint
from gestalt.comms.datagram.protocols.mti import MtiDatagramProtocol


class MtiDatagramEndpoint(DatagramEndpoint):

    protocol_class = MtiDatagramProtocol
