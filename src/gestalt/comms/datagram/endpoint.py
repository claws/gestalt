import asyncio
import logging
import socket

from gestalt import serialization
from gestalt.comms.datagram.protocols.base import BaseDatagramProtocol
from typing import Any, List, Sequence, Tuple

logger = logging.getLogger(__name__)


class DatagramEndpoint(object):
    """
    UDP is connectionless transport so there are no connection events expected.
    In some circumstances, it is possible to receive an error if the transport
    detects that it could not deliver a message. The datagram protocol's
    :meth:`error_received` will get called on the next read or write attempt.
    """

    # Concrete endpoint implementations must define the protocol object to
    # be instantiated to handle a connection. The protocol is
    # expected to inherit from the
    # :ref:`gestalt.comms.datagram.protocols.base.BaseDatagramProtocol` interface.
    protocol_class = None

    def __init__(
        self,
        on_message=None,
        on_started=None,
        on_stopped=None,
        on_peer_available=None,
        on_peer_unavailable=None,
        content_type: str = serialization.CONTENT_TYPE_DATA,
        loop=None,
        **kwargs,
    ):
        self.loop = loop or asyncio.get_event_loop()
        self._on_message_handler = on_message
        self._on_started_handler = on_started
        self._on_stopped_handler = on_stopped
        self._on_peer_available_handler = on_peer_available
        self._on_peer_unavailable_handler = on_peer_unavailable

        self.content_type = content_type
        self.serialization_name = serialization.registry.type_to_name[content_type]
        codec = serialization.registry.get_codec(self.serialization_name)
        self.content_encoding = codec.content_encoding

        if not issubclass(self.protocol_class, BaseDatagramProtocol):
            raise Exception(
                f"Endpoint protocol class must be a subclass of BaseDatagramProtocol, got {self.protocol_class}"
            )

        self._running = False
        self._remote = False
        self._protocol = None

    @property
    def running(self):
        """ Return the running state of the endpoint.

        A running endpoint simply indicates that the endpoint has been started.
        A running client endpoint may not actually be connected as it may be
        attempting reconnects, etc.
        """
        return self._running

    @property
    def bindings(self) -> Sequence[Tuple[str, int]]:
        """ Return a server endpoint's bound addresses. """
        return [self._protocol.laddr] if self.running else []

    @property
    def connections(self) -> Sequence[Tuple[str, int]]:
        """ Return a client endpoint's connect addresses """
        return [self._protocol.raddr] if self.running else []

    async def start(
        self,
        addr: str = "",
        port: int = 0,
        family: int = socket.AF_INET,
        remote: bool = False,
        reuse_address: bool = False,
        reuse_port: bool = False,
        allow_broadcast: bool = False,
    ) -> None:
        """ Start datagam endpoint.

        :param addr: The address to connect or bind to. Defaults to an empty
          string which means all interfaces.

        :param host: The port to connect or bind to. Defaults to 0 which
          results in an ephemeral port being used.

        :param family: An optional address family integer from the socket module.
          Defaults to socket.AF_INET IPv4.

        :param remote: A boolean flag that determines what kind of datagram
          endpoint to create. If remote is False then bind locally to the
          supplied host and port to listen for datagrams. If remote is True
          then establish an endpoint that communicates with the supplied
          host and port on a remote machine.

        :param reuse_address: tells the kernel to reuse a local socket in
          TIME_WAIT state, without waiting for its natural timeout to expire.
          If not specified will automatically be set to True on Unix.

        :param reuse_port: tells the kernel to allow this endpoint to be bound
          to the same port as other existing endpoints are bound to, so long
          as they all set this flag when being created. This option is not
          supported on Windows and some Unixes. If the SO_REUSEPORT constant
          is not defined then this capability is unsupported.

        :param allow_broadcast: tells the kernel to allow this endpoint to
          send messages to the broadcast address.
        """
        if self.running:
            logger.warning(f"Datagram endpoint is already started")
            return

        logger.debug(f"Starting datagram endpoint")

        self._remote = remote

        await self._open(
            local_addr=(addr, port) if not remote else None,
            remote_addr=(addr, port) if remote else None,
            family=family,
            reuse_address=reuse_address,
            reuse_port=reuse_port,
            allow_broadcast=allow_broadcast,
        )

        self._running = True

    async def stop(self):
        """ Stop datagram endpoint """
        if not self.running:
            logger.warning("Datagram endpoint is already stopped")
            return

        logger.debug("Stopping datagram endpoint")

        if self._protocol:
            self._protocol.close()
            # Allow event loop to briefly iterate so that transport can close
            await asyncio.sleep(0)

        self._addr = ""
        self._port = 0
        self._running = False

        # Don't let poor user code break the library
        try:
            if self._on_stopped_handler:
                self._on_stopped_handler()
        except Exception as exc:
            logger.exception("Error in on_stopped callback method")

    def send(
        self, data: bytes, *, peer_id: bytes = None, type_identifier: int = 0, **kwargs
    ):
        """ Send a message to one or more peers.

        :param data: a bytes object containing the message payload.

        :param peer_id: The unique peer identity to send this message to. If
          no peer_id is specified then send to all peers. For a client
          endpoint, which typically has a single peer, this argument can
          conveniently be left unspecified.

        :param type_identifier: An optional parameter specifying the message
          type identifier. If supplied this integer value will be encoded
          into the message frame header.

        """
        if not self._protocol:
            logger.error(f"No protocol to send message with!")
            return

        content_type, content_encoding, data = serialization.dumps(
            data, self.serialization_name
        )

        if not isinstance(data, bytes):
            logger.error(f"data must be bytes - can't send message. data={data}")
            return

        self._protocol.send(data, type_identifier=type_identifier, **kwargs)

    def _protocol_factory(self):
        """ Return a protocol instance to handle a new peer connection """
        return self.protocol_class(
            on_message=self.on_message,
            on_peer_available=self.on_peer_available,
            on_peer_unavailable=self.on_peer_unavailable,
        )

    async def _open(
        self,
        local_addr: Tuple[str, int] = None,
        remote_addr: Tuple[str, int] = None,
        family: int = socket.AF_INET,
        reuse_address: bool = False,
        reuse_port: bool = False,
        allow_broadcast: bool = False,
    ) -> None:
        """
        Open a datagram endpoint.

        :param family: An optional address family integer from the socket module.
          Defaults to socket.AF_INET IPv4.
        """
        try:
            _transport, _protocol = await self.loop.create_datagram_endpoint(
                self._protocol_factory,
                local_addr=local_addr,
                remote_addr=remote_addr,
                family=family,
                reuse_address=reuse_address,
                reuse_port=reuse_port,
                allow_broadcast=allow_broadcast,
            )

            try:
                if self._on_started_handler:
                    self._on_started_handler(self)
            except Exception as exc:
                logger.exception("Error in on_started callback method")

        except (ConnectionRefusedError, OSError) as exc:
            logger.error(f"Connection to {addr}:{port} was refused: {exc}")
        except Exception as exc:
            logger.exception(f"Unexpected error binding to {addr}:{port}: {exc}")

    # async def _listen(
    #     self,
    #     addr: str,
    #     port: int,
    #     family: int = socket.AF_INET,
    #     reuse_address: bool = False,
    #     reuse_port: bool = False,
    #     allow_broadcast: bool = False,
    # ) -> None:
    #     """
    #     Open a datagram endpoint bound to a local address.

    #     :param family: An optional address family integer from the socket module.
    #       Defaults to socket.AF_INET IPv4.
    #     """
    #     try:
    #         _transport, _protocol = await self.loop.create_datagram_endpoint(
    #             self._protocol_factory,
    #             local_addr=(addr, port),
    #             family=family,
    #             reuse_address=self.reuse_address,
    #             reuse_port=self.reuse_port,
    #             allow_broadcast=self.allow_broadcast)

    #         try:
    #             if self._on_started_handler:
    #                 self._on_started_handler(self)
    #         except Exception as exc:
    #             logger.exception("Error in on_started callback method")

    #     except (ConnectionRefusedError, OSError) as exc:
    #         logger.error(f"Connection to {addr}:{port} was refused: {exc}")
    #     except Exception as exc:
    #         logger.exception(f"Unexpected error binding to {addr}:{port}: {exc}")

    def on_peer_available(self, prot, peer_id: bytes):
        """ Called from a protocol instance when its transport is available.

        This means that the peer is ready for sending or receiving messages.

        :param prot: The protocol instance responsible for the peer.

        :param peer_id: The peer's unique identity.

        :param peer_id: A unique peer identity that can be used to route
          messages to the peer.
        """
        self._protocol = prot
        try:
            if self._on_peer_available_handler:
                self._on_peer_available_handler(self, peer_id)
        except Exception as exc:
            logger.exception("Error in on_peer_available callback method")

    def on_peer_unavailable(self, prot, peer_id: bytes):
        """ Called from a protocol instance when its transport is no longer
        available. No further messages can be sent or received from the peer.

        :param prot: The protocol instance responsible for the peer.

        :param peer_id: The peer's unique identity.
        """
        if self._protocol:
            self._protocol = None

        try:
            if self._on_peer_unavailable_handler:
                self._on_peer_unavailable_handler(self, peer_id)
        except Exception as exc:
            logger.exception("Error in on_peer_unavailable callback method")

    def on_message(self, prot, peer_id: bytes, data: bytes, **kwargs) -> None:
        """ Called by a protocol when it receives a message from a peer.

        :param prot: The protocol instance the received the message.

        :param peer_id: The peer's unique identity which can be used to route
          messages back to the originator.

        :param data: The message payload.
        """
        try:
            if self._on_message_handler:
                # addr = kwargs.get("addr")
                type_identifier = kwargs.get("type_identifier")
                data = serialization.loads(
                    data,
                    content_type=self.content_type,
                    content_encoding=self.content_encoding,
                    type_identifier=type_identifier,
                )

                self._on_message_handler(data, peer_id=peer_id, **kwargs)
        except Exception as exc:
            logger.exception("Error in on_message callback method")


# class DatagramServer(DatagramEndpoint):
#     """ An endpoint configured to operate as a server """

#     is_server = True


# class DatagramClient(DatagramEndpoint):
#     """ An endpoint configured to operate as a client """
