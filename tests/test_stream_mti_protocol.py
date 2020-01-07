import logging
import struct
import unittest
import unittest.mock

from gestalt.stream.protocols.mti import MTI_HEADER_FORMAT, MtiStreamProtocol


def create_mti_message(msg_id: int, data: bytes) -> bytes:
    header = struct.pack(MTI_HEADER_FORMAT, len(data), msg_id)
    msg = header + data
    return msg


class MtiStreamProtocolTestCase(unittest.TestCase):
    def test_error_raised_when_sending_invalid_data_type(self):
        p = MtiStreamProtocol()
        with self.assertLogs(
            "gestalt.stream.protocols.mti", level=logging.ERROR
        ) as log:
            p.send("Hello World")
        self.assertIn("data must be bytes", log.output[0])

    def test_error_raised_when_using_invalid_type_identifier_data_type(self):
        p = MtiStreamProtocol()
        with self.assertLogs(
            "gestalt.stream.protocols.mti", level=logging.ERROR
        ) as log:
            p.send(b"Hello World", type_identifier="abc")
        self.assertIn("type_identifier must be integer", log.output[0])

    def test_empty_message_can_be_sent(self):
        p = MtiStreamProtocol()
        transport_mock = unittest.mock.Mock()
        p.transport = transport_mock

        p.send(b"")

        self.assertTrue(transport_mock.write.called)

    def test_empty_message_can_be_received(self):
        on_message_mock = unittest.mock.Mock()

        p = MtiStreamProtocol(on_message=on_message_mock,)

        mti_msg = create_mti_message(42, b"")

        p.data_received(mti_msg)

        self.assertTrue(on_message_mock.called)
        self.assertEqual(on_message_mock.call_count, 1)

    def test_message_received_in_worst_case_delivery_scenario(self):
        on_message_mock = unittest.mock.Mock()

        p = MtiStreamProtocol(on_message=on_message_mock,)

        mti_msg = create_mti_message(42, b"Hello World")

        # Send the test message 1 byte at a time
        for b in mti_msg:
            p.data_received([b])

        self.assertTrue(on_message_mock.called)
        self.assertEqual(on_message_mock.call_count, 1)
