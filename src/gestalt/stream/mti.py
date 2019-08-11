"""
The Message Type Identifier (MTI) enhances an endpoint with high level
awareness of the messages it is transfering. This assists automatic encoding
and decoding actions and can be useful to receiving applications that only
want to process specific message kinds.

The MTI protocol delimits separate messages on the stream using a frame header
that wraps each user message. The message framing strategy adds a length field
and a message type identifier field to the message payload.

This framing strategy allows a receiving application to extract a message
payload from a stream and the message type identifer value in the header
provides additional context about what type of message is in the payload.
This information can be used to help deserialize the data and convert
(unmarshall) it into a convenient structure.

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
