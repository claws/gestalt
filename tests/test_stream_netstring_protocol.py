import logging
import struct
import unittest
import unittest.mock

from gestalt.stream.protocols.netstring import (
    NETSTRING_HEADER_FORMAT,
    NetstringStreamProtocol,
)


def create_netstring_message(data: bytes) -> bytes:
    header = struct.pack(NETSTRING_HEADER_FORMAT, len(data))
    msg = header + data
    return msg


class NetstringStreamProtocolTestCase(unittest.TestCase):
    def test_error_raised_when_sending_invalid_data_type(self):
        p = NetstringStreamProtocol()
        with self.assertLogs(
            "gestalt.stream.protocols.netstring", level=logging.ERROR
        ) as log:
            p.send("Hello World")
        self.assertIn("data must be bytes", log.output[0])

    def test_error_raised_when_sending_empty_message(self):
        p = NetstringStreamProtocol()
        transport_mock = unittest.mock.Mock()
        p.transport = transport_mock

        with self.assertLogs(
            "gestalt.stream.protocols.netstring", level=logging.ERROR
        ) as log:
            p.send(b"")
        self.assertIn("data must contain at least 1 byte", log.output[0])

        self.assertFalse(transport_mock.write.called)

    def test_error_raised_when_received_an_empty_message(self):
        on_message_mock = unittest.mock.Mock()
        close_mock = unittest.mock.Mock()

        p = NetstringStreamProtocol(on_message=on_message_mock,)
        p.close = close_mock

        netstring_msg = create_netstring_message(b"")

        # An empty message is considered invalid
        with self.assertLogs(
            "gestalt.stream.protocols.netstring", level=logging.ERROR
        ) as log:
            p.data_received(netstring_msg)
        self.assertIn("is zero", log.output[0])

        self.assertFalse(on_message_mock.called)

        # Error scenario should trigger the protocol to close the connection
        self.assertTrue(close_mock.called)

    def test_message_received_in_worst_case_delivery_scenario(self):
        on_message_mock = unittest.mock.Mock()

        p = NetstringStreamProtocol(on_message=on_message_mock,)

        netstring_msg = create_netstring_message(b"Hello World")

        # Send the test message 1 byte at a time
        for b in netstring_msg:
            p.data_received([b])

        self.assertTrue(on_message_mock.called)
        self.assertEqual(on_message_mock.call_count, 1)
