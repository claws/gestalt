"""
The netstring endpoint uses a protocol that delimits separate messages on
the stream using a frame header that wraps each user message. The netstring
frame header consists of a single uint32 field containing a value the
represents the number of bytes in the payload.

.. code-block:: console

    +-----------------+----------------------+
    |  header         |  payload             |
    +-----------------+----------------------+
    |  Message_Length |  DATA ....           |
    |     uint32      |                      |
    +-----------------+----------------------+

Messages with a payload size of zero are invalid.

"""

from gestalt.stream.endpoint import StreamClient, StreamServer
from gestalt.stream.protocols.netstring import NetstringStreamProtocol


class NetstringStreamClient(StreamClient):

    protocol_class = NetstringStreamProtocol


class NetstringStreamServer(StreamServer):

    protocol_class = NetstringStreamProtocol
