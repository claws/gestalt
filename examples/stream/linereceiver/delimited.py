import logging
from gestalt.stream.endpoint import StreamClient
from gestalt.stream.endpoint import StreamServer
from gestalt.stream.protocols.base import BaseStreamProtocol


logger = logging.getLogger(__name__)


class LineDelimitedStreamProtocol(BaseStreamProtocol):
    def __init__(
        self,
        on_message=None,
        on_peer_available=None,
        on_peer_unavailable=None,
        delimiter=b"\n",
        **kwargs,
    ):
        super().__init__(
            on_message=on_message,
            on_peer_available=on_peer_available,
            on_peer_unavailable=on_peer_unavailable,
        )
        self.delimiter = delimiter
        self._buffer = bytearray()

    def send(self, data: bytes, **kwargs):
        """ Sends a message by writing it to the transport.

        Messages with zero bytes are not sent as they are considered invalid.

        :param data: a bytes object containing the message payload.
        """
        if not isinstance(data, bytes):
            logger.error(f"data must be bytes - can't send message. data={type(data)}")
            return

        if data:
            logger.debug(f"Sending msg with {len(data)} bytes")
            self.transport.write(data + self.delimiter)

    def data_received(self, data):
        """ Process some bytes received from the transport.

        Upon receiving some bytes from the stream they are added to a buffer
        and then an attempt is made to extract any messages in the buffer
        by splitting on the delimiter.

        This method should support the worst case scenario of receiving a
        single byte at a time, however, a more likely scenario is receiving
        one or more messages at once.
        """
        self._buffer.extend(data)

        msgs = self._buffer.split(self.delimiter)

        # When the buffer ends with a delimiter the split produces an empty
        # bytearray. Discard the trailing empty element and clear the buffer.
        if msgs[-1] == bytearray():
            self._buffer.clear()
            msgs.pop(-1)
        else:
            # A partial msg was in buffer, return it to the buffer.
            self._buffer = msgs.pop(-1)

        for msg in msgs:
            try:
                if self._on_message_handler:
                    self._on_message_handler(self, self._identity, msg)
            except Exception:
                logger.exception("Error in on_message callback method")


class LineDelimitedStreamServer(StreamServer):

    protocol_class = LineDelimitedStreamProtocol


class LineDelimitedStreamClient(StreamClient):

    protocol_class = LineDelimitedStreamProtocol
