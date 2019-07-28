import asyncio
import binascii
import enum
import logging
import os
import struct

from .base import BaseDatagramProtocol

logger = logging.getLogger(__name__)


MTI_HEADER_FORMAT = "II"
MTI_HEADER_SIZE = struct.calcsize(MTI_HEADER_FORMAT)


class MtiDatagramProtocol(BaseDatagramProtocol):
    """
    The Message Type Identifier (MTI) protocol uses a message framing strategy
    when sending and receiving messages. The message framing strategy adds a
    length field and a message type identifier field to the message payload.

    This framing strategy allows a receiving application to extract a message
    from a datagram and the message type identifer provides additional context
    about what type of message is in the payload. This information can be used
    to help unmarshall the data into a convenient structure.

    The frame header consists of two uint32 fields. The value in the first
    field represents the number of bytes in the payload. The second field is
    used to store a message type identifier that can be used by a recipient to
    identify different message types flowing over the stream. Using this field
    is optional.

    .. code-block:: console

        +-----------------------------+--------------------+
        |             header          |  payload           |
        +-----------------------------+--------------------+
        | Message_Length | Message_Id |  DATA ....         |
        |     uint32     |   uint32   |                    |
        |----------------|------------|--------------------|

    Conveniently, messages with a payload size of zero are allowed. This
    results in just the message framing header being sent which transfers
    the message identifier. This can be used to notify recipients of simple
    events that do no need any extra context.

    Upon extracting a message from the stream the mti protocol passes the
    message payload data to the on_message handler along with the optional
    message identifier.
    """

    def send(self, data: bytes, addr=None, type_identifier: int = 0, **kwargs):
        """ Sends a message by writing it to the transport.

        :param data: a bytes object containing the message payload.

        :param addr: The address of the remote endpoint as a (host, port)
          tuple. If remote_addr was specified when the endpoint was created then
          the addr is optional.

        :param type_identifier: a message type identifier.
        """
        if not isinstance(data, bytes):
            logger.error(f"data must be bytes - can't send message. data={type(data)}")
            return

        if not isinstance(type_identifier, int):
            logger.error(
                f"type_identifier must be integer - can't send message. type_identifier={type(type_identifier)}"
            )
            return

        header = struct.pack(MTI_HEADER_FORMAT, len(data), type_identifier)
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
        msg_len, msg_id = struct.unpack(MTI_HEADER_FORMAT, data[:MTI_HEADER_SIZE])
        if msg_len == 0:
            # msg has no body
            msg = ""
        else:
            eom = MTI_HEADER_SIZE + msg_len
            msg = data[MTI_HEADER_SIZE:eom]

        try:
            if self._on_message_handler:
                self._on_message_handler(
                    self, self._identity, msg, addr=addr, type_identifier=msg_id
                )
        except Exception as exc:
            logger.exception("Error in on_message callback method")
