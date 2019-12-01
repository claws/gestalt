import enum
import logging
import struct

from .base import BaseStreamProtocol

logger = logging.getLogger(__name__)


NETSTRING_HEADER_FORMAT = "I"
NETSTRING_HEADER_SIZE = struct.calcsize(NETSTRING_HEADER_FORMAT)


MAX_MSG_SIZE = 2 ** 31 - 1  # limit maximum msg size as a precaution


class ProtocolStates(enum.Enum):
    WAIT_HEADER = 0
    WAIT_PAYLOAD = 1


class NetstringStreamProtocol(BaseStreamProtocol):
    """
    The netstring protocol implements a message framing strategy for
    sending and receiving network messages. The netstring frame header is
    added and removed by this protocol. The netstring frame header
    consists of a single uint32 field containing a value that represents
    the number of bytes in the payload.

    .. code-block:: console

        +-----------------+----------------------+
        |  header         |  payload             |
        +-----------------+----------------------+
        |  Message_Length |  DATA ....           |
        |     uint32      |                      |
        +-----------------+----------------------+

    Messages with a payload size of zero are invalid.

    Upon extracting a message from the stream the protocol passes the message
    payload data to the on_message handler.

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

    def send(
        self, data: bytes, add_frame_header=True, **kwargs
    ):  # pylint: disable=arguments-differ
        """ Sends a message by writing it to the transport.

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

        self.transport.write(msg)

    def data_received(self, data):
        """ Process some bytes received from the transport.

        Upon receiving some bytes from the stream they are added to a buffer
        and then any messages in the buffer are extracted. The parser switches
        between a state where it is attempting to extract a frame header and
        a state where it is attempting to extract a message payload.

        This method should support the worst case scenario of receiving a
        single byte at a time, however, a more likely scenario is receiving
        one or more messages at once.
        """
        self._buffer.extend(data)

        # There could be one byte or multiple messages in the buffer. Process
        # all messages in the buffer.
        while self._buffer:
            if self._state == ProtocolStates.WAIT_HEADER:
                if len(self._buffer) >= NETSTRING_HEADER_SIZE:
                    # Remember that msg_len value represents the length of
                    # the payload, not the total message length which has the
                    # frame header too.
                    msg_len = struct.unpack(
                        NETSTRING_HEADER_FORMAT, self._buffer[:NETSTRING_HEADER_SIZE]
                    )[0]

                    if msg_len == 0 or msg_len > MAX_MSG_SIZE:
                        # msg has no body or is too big
                        logger.error(
                            f"Msg size ({msg_len}) is zero or exceeds maximum msg size. "
                            f"Disconnecting peer {self._identity}."
                        )
                        self.close()
                        break
                    else:
                        # The buffer holds more than a header so switch to
                        # waiting for msg payload
                        self._state = ProtocolStates.WAIT_PAYLOAD
                else:
                    # There is not enough bytes to extract the header yet.
                    break

            elif self._state == ProtocolStates.WAIT_PAYLOAD:
                eom = NETSTRING_HEADER_SIZE + msg_len
                if len(self._buffer) >= eom:
                    msg = bytes(self._buffer[NETSTRING_HEADER_SIZE:eom])
                    self._buffer = self._buffer[eom:]
                    self._state = ProtocolStates.WAIT_HEADER

                    # Don't let user code break the library
                    try:
                        if self._on_message_handler:
                            self._on_message_handler(self, self._identity, msg)
                    except Exception:
                        logger.exception("Error in on_message callback method")

                else:
                    # There is not enough bytes to extract the payload yet.
                    break
