import asyncio
import binascii
import enum
import logging
import os
import struct

from .base import BaseDatagramProtocol

logger = logging.getLogger(__name__)


NETSTRING_HEADER_FORMAT = "I"
NETSTRING_HEADER_SIZE = struct.calcsize(NETSTRING_HEADER_FORMAT)


class NetstringDatagramProtocol(BaseDatagramProtocol):
    """
    The netstring protocol implements a message framing strategy that
    wraps a messages with a frame header that consists of a single
    uint32 field containing a value that represents the number of bytes
    in the payload.

    .. code-block:: console

        +-----------------+----------------------+
        |  header         |  payload             |
        +-----------------+----------------------+
        |  Message_Length |  DATA ....           |
        |     uint32      |                      |
        +-----------------+----------------------+

    Messages with a payload size of zero are invalid.
    """

    def send(self, data: bytes, addr=None, add_frame_header=True, **kwargs):
        """
        Send a message to a remote UDP endpoint by writing it to the transport.

        :param data: a bytes object containing the message payload.

        :param addr: The address of the remote endpoint as a (host, port)
          tuple. If remote_addr was specified when the endpoint was created then
          the addr is optional.

        :param add_frame_header: A flag that informs the sending function
          whether it needs to wrap the payload data with the frame header.
          Defaults to True. This parameter should be set to False when sending
          pre-formed messages - such as in a relay type application.
        """
        if not isinstance(data, bytes):
            logger.error(f"data must be bytes - can't send message. data={data}")
            return

        header = struct.pack(NETSTRING_HEADER_FORMAT, len(data))
        msg = header + data

        logger.debug(f"Sending msg with {len(msg)} bytes")

        self.transport.sendto(msg, addr=addr)

    def datagram_received(self, data, addr):
        """
        Process a datagram received from the transport.

        When passing a message up to the endpoint, the datagram protocol
        passes the senders address as an extra kwarg.

        :param data: The datagram payload

        :param addr: A (host, port) tuple defining the source address

        """
        # Remember that msg_len value represents the length of the payload,
        # not the total message length which has the frame header too.
        msg_len = struct.unpack(NETSTRING_HEADER_FORMAT, data[:NETSTRING_HEADER_SIZE])[
            0
        ]
        eom = NETSTRING_HEADER_SIZE + msg_len
        msg = data[NETSTRING_HEADER_SIZE:eom]

        try:
            if self._on_message_handler:
                self._on_message_handler(self, self._identity, msg, addr=addr)
        except Exception as exc:
            logger.exception("Error in on_message callback method")
