import asyncio
import binascii
import logging
import os

from typing import Tuple


logger = logging.getLogger(__name__)


class BaseDatagramProtocol(asyncio.DatagramProtocol):
    """ Datagram protocol for an endpoint. """

    def __init__(
        self,
        on_message=None,
        on_peer_available=None,
        on_peer_unavailable=None,
        **kwargs,
    ):
        """

        :param on_message: A callback function that will be passed each message
          that the protocol receives.

        :param on_peer_available: A callback function that will be called when
          the protocol is connected with a transport. In this state the protocol
          can send and receive messages.

        :param on_peer_unavailable: A callback function that will be called when
          the protocol has lost the connection with its transport. In this state
          the protocol can not send or receive messages.
        """
        self._on_message_handler = on_message
        self._on_peer_available_handler = on_peer_available
        self._on_peer_unavailable_handler = on_peer_unavailable
        self._identity = b""
        self._remote_address = None  # type: Optional[Tuple[str, int]]
        self._local_address = None  # type: Optional[Tuple[str, int]]
        self.transport = None

    @property
    def identity(self):
        """ Return the protocol's unique identifier """
        return self._identity

    @property
    def raddr(self) -> Tuple[str, int]:
        """ Return the remote address the protocol is connected with """
        return self._remote_address

    @property
    def laddr(self) -> Tuple[str, int]:
        """ Return the local address the protocol is using """
        return self._local_address

    def connection_made(self, transport):
        self.transport = transport
        self._identity = binascii.hexlify(os.urandom(5))
        self._local_address = transport.get_extra_info("sockname")
        self._remote_address = transport.get_extra_info("peername")

        logger.debug(f"UDP protocol connection made. id={self._identity}")

        try:
            if self._on_peer_available_handler:
                self._on_peer_available_handler(self, self._identity)
        except Exception:
            logger.exception("Error in on_peer_available callback method")

    def connection_lost(self, exc):
        """
        Called by the event loop when the protocol is disconnected from a transport.
        """
        logger.debug(f"UDP protocol connection lost. id={self._identity}")
        try:
            if self._on_peer_unavailable_handler:
                self._on_peer_unavailable_handler(self, self._identity)
        except Exception:
            logger.exception("Error in on_peer_unavailable callback method")

        self.transport = None
        self._identity = None
        self._local_address = None

    def close(self):
        """ Close this connection """
        logger.debug(f"Closing connection. id={self._identity}")
        if self.transport:
            self.transport.close()

    def send(self, data, addr=None, **kwargs):
        """
        Send a message to a remote UDP endpoint by writing it to the transport.

        :param data: a bytes object containing the message payload.

        :param addr: The address of the remote endpoint as a (host, port)
          tuple. If remote_addr was specified when the endpoint was created then
          the addr is optional.
        """
        if not isinstance(data, bytes):
            logger.error(f"data must be bytes - can't send message. data={type(data)}")
            return

        self.transport.sendto(data, addr=addr)

    def datagram_received(self, data, addr):
        """
        Process a datagram received from the transport.

        When passing a message up to the endpoint, the datagram protocol
        passes the senders address as an extra kwarg.

        :param data: The datagram payload

        :param addr: A (host, port) tuple defining the source address

        """
        try:
            if self._on_message_handler:
                self._on_message_handler(self, self._identity, data, addr=addr)
        except Exception:
            logger.exception("Error in on_message callback method")

    def error_received(self, exc):
        """
        In many conditions undeliverable datagrams will be silently dropped.
        In some rare conditions the transport can sometimes detect that the
        datagram could not be delivered to the recipient.

        :param exc: an OSError instance.
        """
        logger.error(f"Datagram error: {exc}")
