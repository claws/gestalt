import asyncio
import binascii
import enum
import logging
import os
import struct

from .base import BaseStreamProtocol

logger = logging.getLogger(__name__)


MTI_HEADER_FORMAT = "II"
MTI_HEADER_SIZE = struct.calcsize(MTI_HEADER_FORMAT)


MAX_MSG_SIZE = 2 ** 31 - 1  # limit maximum msg size as a precaution


class ProtocolStates(enum.Enum):
    WAIT_HEADER = 0
    WAIT_PAYLOAD = 1


class MtiStreamProtocol(BaseStreamProtocol):
    """
    The Message Type Identifier (MTI) protocol uses a message framing strategy
    when sending and receiving messages. The message framing strategy adds a
    length field and a message type identifier field to the message payload.

    This framing strategy allows a receiving application to extract a message
    from a stream and the message type identifer provides additional context
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

    Upon extracting a message from the stream the PAL protocol passes the
    message payload data to the on_message handler along with the optional
    message identifier.
    """

    def __init__(
        self,
        on_message=None,
        on_peer_available=None,
        on_peer_unavailable=None,
        **kwargs,
    ):
        super().__init__(
            on_message=on_message,
            on_peer_available=on_peer_available,
            on_peer_unavailable=on_peer_unavailable,
        )
        self._buffer = bytearray()
        self._state = ProtocolStates.WAIT_HEADER

    def send(self, data: bytes, type_identifier: int = 0, **kwargs):
        """ Sends a message by writing it to the transport.

        :param data: a bytes object containing the message payload.

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

        self.transport.write(msg)

    def data_received(self, data):
        """ Process some bytes received from the transport.

        Upon receiving some data it is added to the buffer and then each message
        in the buffer is extracted. The parser switches between a state where
        it is attempting to extract a frame header and a state where it is
        attempting to extract a message payload.

        This method should support receiving a byte at a time, however, a more
        likely scenario is receiving one or more messages at once.
        """
        self._buffer.extend(data)

        # Process all messages in the buffer
        while self._buffer:
            if self._state == ProtocolStates.WAIT_HEADER:
                if len(self._buffer) >= MTI_HEADER_SIZE:
                    # The msg_len value represents the length of the payload.
                    # It does not include the length of the frame header.
                    msg_len, msg_id = struct.unpack(
                        MTI_HEADER_FORMAT, self._buffer[:MTI_HEADER_SIZE]
                    )

                    if msg_len == 0:
                        # msg has no body
                        eom = MTI_HEADER_SIZE
                        self._buffer = self._buffer[eom:]
                        self._state = ProtocolStates.WAIT_HEADER

                        # Don't let user code break the library
                        try:
                            if self._on_message_handler:
                                self._on_message_handler(
                                    self, self._identity, b"", identifier=msg_id
                                )
                        except Exception as exc:
                            logger.exception("Error in on_message callback method")

                    elif msg_len > MAX_MSG_SIZE:
                        logger.error(
                            f"Msg size ({msg_len}) exceeds maximum allowed msg size. "
                            f"Disconnecting peer {self._identity}."
                        )
                        self.close()
                    else:
                        # The buffer holds more then a header so switch to
                        # waiting for msg payload
                        self._state = ProtocolStates.WAIT_PAYLOAD
                else:
                    # There is not enough bytes to extract the header yet.
                    break

            elif self._state == ProtocolStates.WAIT_PAYLOAD:
                eom = MTI_HEADER_SIZE + msg_len
                if len(self._buffer) >= eom:
                    msg = bytes(self._buffer[MTI_HEADER_SIZE:eom])
                    self._buffer = self._buffer[eom:]
                    self._state = ProtocolStates.WAIT_HEADER

                    # Don't let user code break the library
                    try:
                        if self._on_message_handler:
                            self._on_message_handler(
                                self, self._identity, msg, type_identifier=msg_id
                            )
                    except Exception as exc:
                        logger.exception("Error in on_message callback method")

                else:
                    # There is not enough bytes to extract the payload yet.
                    break
