import asyncio
import asynctest
import logging
import socket
import unittest.mock
from gestalt import serialization
from gestalt.datagram.mti import MtiDatagramEndpoint


class MtiDatagramEndpointTestCase(asynctest.TestCase):
    async def test_start_receiver(self):
        receiver_on_message_mock = unittest.mock.Mock()
        receiver_on_started_mock = unittest.mock.Mock()
        receiver_on_stopped_mock = unittest.mock.Mock()
        receiver_on_peer_available_mock = unittest.mock.Mock()
        receiver_on_peer_unavailable_mock = unittest.mock.Mock()

        receiver_ep = MtiDatagramEndpoint(
            on_message=receiver_on_message_mock,
            on_started=receiver_on_started_mock,
            on_stopped=receiver_on_stopped_mock,
            on_peer_available=receiver_on_peer_available_mock,
            on_peer_unavailable=receiver_on_peer_unavailable_mock,
        )

        # Expect an exception if local_addr or remote addr are not
        # specified
        with self.assertRaises(Exception) as cm:
            await receiver_ep.start()
        expected = "At least one of local_addr or remote addr must be defined"
        self.assertIn(expected, str(cm.exception))

        await receiver_ep.start(local_addr=("0.0.0.0", 0))
        self.assertTrue(receiver_on_started_mock.called)
        self.assertTrue(receiver_on_peer_available_mock.called)

        address, port = receiver_ep.bindings[0]

        # Check that starting a receiver that is already started does not
        # have any consequences
        await receiver_ep.start()

        await receiver_ep.stop()
        self.assertTrue(receiver_on_stopped_mock.called)

        # Check that stopping a receiver that is already stopped does not
        # have any consequences
        await receiver_ep.stop()

    async def test_start_receiver_on_unavailable_port(self):
        """ check starting receiver on a used port raises an exception """
        # Occupy a port by starting a UDP endpoint on it first.
        first_ep = MtiDatagramEndpoint()
        await first_ep.start(local_addr=("0.0.0.0", 0))
        host, occupied_port = first_ep.bindings[0]

        try:
            receiver_on_message_mock = unittest.mock.Mock()
            receiver_on_started_mock = unittest.mock.Mock()
            receiver_on_stopped_mock = unittest.mock.Mock()
            receiver_on_peer_available_mock = unittest.mock.Mock()
            receiver_on_peer_unavailable_mock = unittest.mock.Mock()

            receiver_ep = MtiDatagramEndpoint(
                on_message=receiver_on_message_mock,
                on_started=receiver_on_started_mock,
                on_stopped=receiver_on_stopped_mock,
                on_peer_available=receiver_on_peer_available_mock,
                on_peer_unavailable=receiver_on_peer_unavailable_mock,
            )

            with self.assertLogs(
                "gestalt.datagram.endpoint", level=logging.ERROR
            ) as log:
                with self.assertRaises(Exception):
                    await receiver_ep.start(local_addr=(host, occupied_port))
                    address, port = receiver_ep.bindings[0]

            self.assertFalse(receiver_on_started_mock.called)

            await receiver_ep.stop()
            # endpoint never actually started so it should not really need
            # to be stopped.
            self.assertFalse(receiver_on_stopped_mock.called)
        finally:
            await first_ep.stop()

    async def test_sender_receiver_interaction(self):
        """ check sender and receiver interactions """

        receiver_on_message_mock = asynctest.CoroutineMock()
        receiver_on_started_mock = asynctest.CoroutineMock()
        receiver_on_stopped_mock = asynctest.CoroutineMock()
        receiver_on_peer_available_mock = asynctest.CoroutineMock()
        receiver_on_peer_unavailable_mock = asynctest.CoroutineMock()

        receiver_ep = MtiDatagramEndpoint(
            on_message=receiver_on_message_mock,
            on_started=receiver_on_started_mock,
            on_stopped=receiver_on_stopped_mock,
            on_peer_available=receiver_on_peer_available_mock,
            on_peer_unavailable=receiver_on_peer_unavailable_mock,
        )

        await receiver_ep.start(local_addr=("127.0.0.1", 0))
        self.assertTrue(receiver_on_started_mock.called)

        address, port = receiver_ep.bindings[0]

        sender_on_message_mock = asynctest.CoroutineMock()
        sender_on_started_mock = asynctest.CoroutineMock()
        sender_on_stopped_mock = asynctest.CoroutineMock()
        sender_on_peer_available_mock = asynctest.CoroutineMock()
        sender_on_peer_unavailable_mock = asynctest.CoroutineMock()

        sender_ep = MtiDatagramEndpoint(
            on_message=sender_on_message_mock,
            on_started=sender_on_started_mock,
            on_stopped=sender_on_stopped_mock,
            on_peer_available=sender_on_peer_available_mock,
            on_peer_unavailable=sender_on_peer_unavailable_mock,
        )

        await sender_ep.start(remote_addr=(address, port))
        await asyncio.sleep(0.3)

        self.assertTrue(sender_on_started_mock.called)
        self.assertTrue(sender_on_peer_available_mock.called)
        self.assertTrue(receiver_on_peer_available_mock.called)

        # Send a msg without identifier from sender to receiver
        sent_msg = b"Hello World"
        sender_ep.send(sent_msg)
        await asyncio.sleep(0.1)

        self.assertTrue(receiver_on_message_mock.called)
        (args, kwargs) = receiver_on_message_mock.call_args_list[0]
        ep, received_msg = args
        self.assertIsInstance(ep, MtiDatagramEndpoint)
        self.assertEqual(received_msg, sent_msg)
        self.assertIn("addr", kwargs)
        received_sender_id = kwargs["addr"]
        self.assertIn("type_identifier", kwargs)
        received_msg_id = kwargs["type_identifier"]
        self.assertEqual(received_msg_id, 0)

        await sender_ep.stop()
        await asyncio.sleep(0.1)

        self.assertTrue(sender_on_stopped_mock.called)
        self.assertTrue(sender_on_peer_unavailable_mock.called)

        await receiver_ep.stop()
        self.assertTrue(receiver_on_stopped_mock.called)
        self.assertTrue(receiver_on_peer_unavailable_mock.called)

    async def test_json_sender_receiver_interactions(self):
        """ check JSON sender and receiver interactions """

        receiver_on_message_mock = asynctest.CoroutineMock()
        receiver_on_started_mock = asynctest.CoroutineMock()
        receiver_on_stopped_mock = asynctest.CoroutineMock()
        receiver_on_peer_available_mock = asynctest.CoroutineMock()
        receiver_on_peer_unavailable_mock = asynctest.CoroutineMock()

        receiver_ep = MtiDatagramEndpoint(
            on_message=receiver_on_message_mock,
            on_started=receiver_on_started_mock,
            on_stopped=receiver_on_stopped_mock,
            on_peer_available=receiver_on_peer_available_mock,
            on_peer_unavailable=receiver_on_peer_unavailable_mock,
            content_type=serialization.CONTENT_TYPE_JSON,
        )

        await receiver_ep.start(local_addr=("127.0.0.1", 0))
        self.assertTrue(receiver_on_started_mock.called)

        address, port = receiver_ep.bindings[0]

        sender_on_message_mock = asynctest.CoroutineMock()
        sender_on_started_mock = asynctest.CoroutineMock()
        sender_on_stopped_mock = asynctest.CoroutineMock()
        sender_on_peer_available_mock = asynctest.CoroutineMock()
        sender_on_peer_unavailable_mock = asynctest.CoroutineMock()

        sender_ep = MtiDatagramEndpoint(
            on_message=sender_on_message_mock,
            on_started=sender_on_started_mock,
            on_stopped=sender_on_stopped_mock,
            on_peer_available=sender_on_peer_available_mock,
            on_peer_unavailable=sender_on_peer_unavailable_mock,
            content_type=serialization.CONTENT_TYPE_JSON,
        )

        await sender_ep.start(remote_addr=(address, port))
        await asyncio.sleep(0.3)

        self.assertTrue(sender_on_started_mock.called)
        self.assertTrue(sender_on_peer_available_mock.called)
        self.assertTrue(receiver_on_peer_available_mock.called)

        # Send a msg without identifier from sender to receiver
        test_msg_in = dict(latitude=130.0, longitude=-30.0, altitude=50.0)
        sender_ep.send(test_msg_in)
        await asyncio.sleep(0.1)

        self.assertTrue(receiver_on_message_mock.called)
        (args, kwargs) = receiver_on_message_mock.call_args_list[0]
        ep, received_msg = args
        received_sender_id = kwargs["addr"]
        received_msg_id = kwargs["type_identifier"]

        self.assertIsInstance(ep, MtiDatagramEndpoint)
        self.assertEqual(received_msg, test_msg_in)
        self.assertEqual(received_msg_id, 0)

        # Send a msg with identifier from sender to receiver
        receiver_on_message_mock.reset_mock()

        type_identifier = 2
        sender_ep.send(test_msg_in, type_identifier=type_identifier)
        await asyncio.sleep(0.1)

        self.assertTrue(receiver_on_message_mock.called)
        (args, kwargs) = receiver_on_message_mock.call_args_list[0]
        ep, received_msg = args
        received_sender_id = kwargs["addr"]
        received_msg_id = kwargs["type_identifier"]

        self.assertIsInstance(ep, MtiDatagramEndpoint)
        self.assertEqual(received_msg, test_msg_in)
        self.assertEqual(received_msg_id, type_identifier)

        # graceful shutdown
        await sender_ep.stop()
        await asyncio.sleep(0.1)

        self.assertTrue(sender_on_stopped_mock.called)
        self.assertTrue(sender_on_peer_unavailable_mock.called)

        await receiver_ep.stop()
        self.assertTrue(receiver_on_stopped_mock.called)
        self.assertTrue(receiver_on_peer_unavailable_mock.called)

    @unittest.skipUnless(serialization.have_msgpack, "requires msgpack")
    async def test_msgpack_sender_receiver_interactions(self):
        """ check msgpack sender and receiver interactions """

        receiver_on_message_mock = asynctest.CoroutineMock()
        receiver_on_started_mock = asynctest.CoroutineMock()
        receiver_on_stopped_mock = asynctest.CoroutineMock()
        receiver_on_peer_available_mock = asynctest.CoroutineMock()
        receiver_on_peer_unavailable_mock = asynctest.CoroutineMock()

        receiver_ep = MtiDatagramEndpoint(
            on_message=receiver_on_message_mock,
            on_started=receiver_on_started_mock,
            on_stopped=receiver_on_stopped_mock,
            on_peer_available=receiver_on_peer_available_mock,
            on_peer_unavailable=receiver_on_peer_unavailable_mock,
            content_type=serialization.CONTENT_TYPE_MSGPACK,
        )

        await receiver_ep.start(local_addr=("127.0.0.1", 0))
        self.assertTrue(receiver_on_started_mock.called)

        address, port = receiver_ep.bindings[0]

        sender_on_message_mock = asynctest.CoroutineMock()
        sender_on_started_mock = asynctest.CoroutineMock()
        sender_on_stopped_mock = asynctest.CoroutineMock()
        sender_on_peer_available_mock = asynctest.CoroutineMock()
        sender_on_peer_unavailable_mock = asynctest.CoroutineMock()

        sender_ep = MtiDatagramEndpoint(
            on_message=sender_on_message_mock,
            on_started=sender_on_started_mock,
            on_stopped=sender_on_stopped_mock,
            on_peer_available=sender_on_peer_available_mock,
            on_peer_unavailable=sender_on_peer_unavailable_mock,
            content_type=serialization.CONTENT_TYPE_MSGPACK,
        )

        await sender_ep.start(remote_addr=(address, port))
        await asyncio.sleep(0.3)

        self.assertTrue(sender_on_started_mock.called)
        self.assertTrue(sender_on_peer_available_mock.called)
        self.assertTrue(receiver_on_peer_available_mock.called)

        # Send a msg without identifier from sender to receiver
        test_msg_in = dict(latitude=130.0, longitude=-30.0, altitude=50.0)
        sender_ep.send(test_msg_in)
        await asyncio.sleep(0.1)

        self.assertTrue(receiver_on_message_mock.called)
        (args, kwargs) = receiver_on_message_mock.call_args_list[0]
        ep, received_msg = args
        received_sender_id = kwargs["addr"]
        received_msg_id = kwargs["type_identifier"]

        self.assertIsInstance(ep, MtiDatagramEndpoint)
        self.assertEqual(received_msg, test_msg_in)
        self.assertEqual(received_msg_id, 0)

        # Send a msg with identifier from sender to receiver
        receiver_on_message_mock.reset_mock()

        type_identifier = 2
        sender_ep.send(test_msg_in, type_identifier=type_identifier)
        await asyncio.sleep(0.1)

        self.assertTrue(receiver_on_message_mock.called)
        (args, kwargs) = receiver_on_message_mock.call_args_list[0]
        ep, received_msg = args
        received_sender_id = kwargs["addr"]
        received_msg_id = kwargs["type_identifier"]

        self.assertIsInstance(ep, MtiDatagramEndpoint)
        self.assertEqual(received_msg, test_msg_in)
        self.assertEqual(received_msg_id, type_identifier)

        # graceful shutdown
        await sender_ep.stop()
        await asyncio.sleep(0.1)

        self.assertTrue(sender_on_stopped_mock.called)
        self.assertTrue(sender_on_peer_unavailable_mock.called)

        await receiver_ep.stop()
        self.assertTrue(receiver_on_stopped_mock.called)
        self.assertTrue(receiver_on_peer_unavailable_mock.called)

    @unittest.skipUnless(serialization.have_protobuf, "requires google protobuf")
    async def test_protobuf_sender_receiver_interactions(self):
        """ check protobuf sender and receiver interactions """
        from position_pb2 import Position

        protobuf_data = Position(
            latitude=130.0, longitude=-30.0, altitude=50.0, status=Position.SIMULATED
        )

        receiver_on_message_mock = asynctest.CoroutineMock()
        receiver_on_started_mock = asynctest.CoroutineMock()
        receiver_on_stopped_mock = asynctest.CoroutineMock()
        receiver_on_peer_available_mock = asynctest.CoroutineMock()
        receiver_on_peer_unavailable_mock = asynctest.CoroutineMock()

        receiver_ep = MtiDatagramEndpoint(
            on_message=receiver_on_message_mock,
            on_started=receiver_on_started_mock,
            on_stopped=receiver_on_stopped_mock,
            on_peer_available=receiver_on_peer_available_mock,
            on_peer_unavailable=receiver_on_peer_unavailable_mock,
            content_type=serialization.CONTENT_TYPE_PROTOBUF,
        )

        await receiver_ep.start(local_addr=("127.0.0.1", 0))
        self.assertTrue(receiver_on_started_mock.called)

        address, port = receiver_ep.bindings[0]

        sender_on_message_mock = asynctest.CoroutineMock()
        sender_on_started_mock = asynctest.CoroutineMock()
        sender_on_stopped_mock = asynctest.CoroutineMock()
        sender_on_peer_available_mock = asynctest.CoroutineMock()
        sender_on_peer_unavailable_mock = asynctest.CoroutineMock()

        sender_ep = MtiDatagramEndpoint(
            on_message=sender_on_message_mock,
            on_started=sender_on_started_mock,
            on_stopped=sender_on_stopped_mock,
            on_peer_available=sender_on_peer_available_mock,
            on_peer_unavailable=sender_on_peer_unavailable_mock,
            content_type=serialization.CONTENT_TYPE_PROTOBUF,
        )

        await sender_ep.start(remote_addr=(address, port))
        await asyncio.sleep(0.3)

        self.assertTrue(sender_on_started_mock.called)
        self.assertTrue(sender_on_peer_available_mock.called)
        self.assertTrue(receiver_on_peer_available_mock.called)

        type_identifier = 2
        receiver_ep.register_message(type_identifier, Position)

        # Send a msg with identifier from sender to receiver
        test_msg_in = protobuf_data
        sender_ep.send(test_msg_in, type_identifier=type_identifier)
        await asyncio.sleep(0.1)

        self.assertTrue(receiver_on_message_mock.called)
        (args, kwargs) = receiver_on_message_mock.call_args_list[0]
        ep, received_msg = args
        received_sender_id = kwargs["addr"]
        received_msg_id = kwargs["type_identifier"]

        self.assertIsInstance(ep, MtiDatagramEndpoint)
        self.assertEqual(received_msg, test_msg_in)
        self.assertEqual(received_msg_id, type_identifier)

        await sender_ep.stop()
        await asyncio.sleep(0.1)

        self.assertTrue(sender_on_stopped_mock.called)
        self.assertTrue(sender_on_peer_unavailable_mock.called)

        await receiver_ep.stop()
        self.assertTrue(receiver_on_stopped_mock.called)
        self.assertTrue(receiver_on_peer_unavailable_mock.called)
