"""
The Message Type Identifier (MTI) endpoint uses a protocol that delimits
separate messages on the srtream using a frame header that wraps each user
message. The message framing strategy adds a length field and a message type
identifier field to the message payload.

This framing strategy allows a receiving application to extract a message
paylod from a stream and the message type identifer value in the header
provides additional context about what type of message is in the payload.
This information can be used to help deserialize the data and convert
(unmarshall) it into a convenient structure.

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
"""
import logging
import os

from gestalt import serialization
from gestalt.stream.endpoint import StreamClient, StreamServer
from gestalt.stream.protocols.mti import MtiStreamProtocol


logger = logging.getLogger(__name__)


class MtiStreamClient(StreamClient):

    protocol_class = MtiStreamProtocol


class MtiStreamServer(StreamServer):

    protocol_class = MtiStreamProtocol
