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

import asyncio
import binascii
import enum
import logging
import os
import struct

from gestalt.comms.stream.endpoint import Client, Server
from gestalt.comms.stream.protocols.netstring import NetstringProtocol

logger = logging.getLogger(__name__)


class NetstringClient(Client):

    protocol_class = NetstringProtocol


class NetstringServer(Server):

    protocol_class = NetstringProtocol
