import asyncio
import binascii
import enum
import logging
import os
import struct


logger = logging.getLogger(__name__)


class BaseStreamProtocol(asyncio.Protocol):
    """
    This class implements the minimum interface expected by Endpoint.

    This protocol implementation is not capable of extracting atomic messages
    from a stream. It simply transfers any received bytes to the message
    handler callback. Users of this class need to implement a message parser
    themself.
    """

    def __init__(
        self,
        on_message=None,
        on_peer_available=None,
        on_peer_unavailable=None,
        **kwargs,
    ):
        """

        :param on_message: A callback function that will be passed each message
          that the protocol extracts from the stream.

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
        self._remote_address = None
        self._local_address = None
        self._peercert = None
        self._identity = b""

    @property
    def raddr(self):
        """ Return the remote address the protocol is connected with """
        return self._remote_address

    @property
    def laddr(self):
        """ Return the local address the protocol is using """
        return self._local_address

    @property
    def identity(self):
        """ Return the protocol's unique identifier.

        A protocol's unique identity provides a method for routing a message
        (e.g. a response) to a specific peer (e.g. the originator).
        """
        return self._identity

    def connection_made(self, transport):
        """
        Called by the event loop when the protocol is connected with a transport.
        """
        self.transport = transport
        self._remote_address = transport.get_extra_info("peername")
        self._local_address = transport.get_extra_info("sockname")
        self._peercert = transport.get_extra_info("peercert")
        self._identity = binascii.hexlify(os.urandom(5))

        logger.debug(
            f"Connection made. id={self._identity}, "
            f"laddr={self._local_address}, "
            f"raddr={self._remote_address}, "
            f"peercert={self._peercert}"
        )

        # Don't let user code break the library
        try:
            if self._on_peer_available_handler:
                self._on_peer_available_handler(self, self._identity)
        except Exception as exc:
            logger.exception("Error in on_peer_available callback method")

    def connection_lost(self, exc):
        """
        Called by the event loop when the protocol is disconnected from a transport.
        """
        logger.debug(
            f"Connection lost. id={self._identity}, "
            f"laddr={self._local_address}, "
            f"raddr={self._remote_address}"
        )

        # Don't let user code break the library
        try:
            if self._on_peer_unavailable_handler:
                self._on_peer_unavailable_handler(self, self._identity)
        except Exception as exc:
            logger.exception("Error in on_peer_unavailable callback method")

        if hasattr(self, "transport"):
            if self.transport:
                self.transport.close()  # resolves a sslproto.py related warning.

        self.transport = None
        self._remote_address = None
        self._local_address = None
        self._identity = None

    def close(self):
        """
        Close this connection.
        """
        logger.debug(
            f"Closing connection. id={self._identity}, "
            f"laddr={self._local_address}, raddr={self._remote_address}"
        )

        if self.transport:
            self.transport.close()

    def send(self, data: bytes, **kwargs):
        """ Sends a message by writing it to the transport.

        :param data: a bytes object containing the message payload.
        """
        if not isinstance(data, bytes):
            logger.error(f"data must be bytes - can't send message. data={type(data)}")
            return

        logger.debug(f"Sending msg with {len(data)} bytes")

        self.transport.write(data)

    def data_received(self, data):
        """ Process some bytes received from the transport."""
        # Don't let user code break the library
        try:
            if self._on_message_handler:
                self._on_message_handler(self, self._identity, msg)
        except Exception as exc:
            logger.exception("Error in on_message callback method")
