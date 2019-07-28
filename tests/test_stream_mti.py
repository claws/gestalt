import asyncio
import asynctest
import logging
import socket
import unittest.mock
import typing
from gestalt import serialization
from gestalt.stream.mti import MtiStreamClient, MtiStreamServer


class MtiStreamEndpointTestCase(asynctest.TestCase):
    async def test_start_server(self):
        """ Check a MTI server can be started """
        server_on_started_mock = unittest.mock.Mock()
        server_on_stopped_mock = unittest.mock.Mock()

        server_ep = MtiStreamServer(
            on_started=server_on_started_mock, on_stopped=server_on_stopped_mock
        )

        await server_ep.start()
        self.assertTrue(server_on_started_mock.called)
        self.assertEqual(
            server_on_started_mock.call_args, unittest.mock.call(server_ep)
        )

        address, port = server_ep.bindings[0]

        # Check that starting a server that is already started does not
        # have any consequences
        await server_ep.start()

        await server_ep.stop()
        self.assertTrue(server_on_stopped_mock.called)

        # Check that stopping a server that is already stopped does not
        # have any consequences
        await server_ep.stop()
        self.assertEqual(
            server_on_stopped_mock.call_args, unittest.mock.call(server_ep)
        )

    async def test_start_server_on_unavailable_port(self):
        """ check starting server on a used port raises an exception """
        # Occupy a port by starting a server
        listener = await self.loop.create_server(
            asyncio.Protocol, host="", port=0, family=socket.AF_INET
        )
        host, occupied_port = listener.sockets[0].getsockname()

        try:
            server_on_started_mock = unittest.mock.Mock()
            server_on_stopped_mock = unittest.mock.Mock()

            server_ep = MtiStreamServer(
                on_started=server_on_started_mock, on_stopped=server_on_stopped_mock
            )

            with self.assertLogs("gestalt.stream.endpoint", level=logging.ERROR) as log:
                with self.assertRaises(Exception):
                    await server_ep.start(addr=host, port=occupied_port)

            self.assertFalse(server_on_started_mock.called)

            await server_ep.stop()
            self.assertTrue(server_on_stopped_mock.called)
        finally:
            listener.close()
            await listener.wait_closed()

    async def test_start_client_with_no_server(self):

        client_on_started_mock = asynctest.CoroutineMock()
        client_on_stopped_mock = asynctest.CoroutineMock()

        client_ep = MtiStreamClient(
            on_started=client_on_started_mock, on_stopped=client_on_stopped_mock
        )

        # Attempt connection with reconnect=True
        sub_tests = (
            # addr_value, description
            ("localhost", "Using localhost as address"),
            ("127.0.0.1", "Using 127.0.0.1 as address"),
        )

        # Attempt connection with default reconnect=True
        for addr_value, subtest_description in sub_tests:
            with self.subTest(subtest_description):
                with self.assertLogs(
                    "gestalt.stream.endpoint", level=logging.INFO
                ) as log:
                    await client_ep.start(addr=addr_value, port=5555)
                    # wait briefly for an expected reconnect attempt
                    await asyncio.sleep(0.1)
                    expected_items = (
                        "was refused",
                        "Attempting reconnect in",
                        "seconds before connection attempt",
                    )
                    for expected_item in expected_items:
                        self.assertTrue(
                            any(expected_item in log_item for log_item in log.output)
                        )

                    await client_ep.stop()

        # Attempt connection with reconnect=False
        for addr_value, subtest_description in sub_tests:
            with self.subTest(subtest_description):
                with self.assertLogs(
                    "gestalt.stream.endpoint", level=logging.DEBUG
                ) as log:
                    await client_ep.start(addr="127.0.0.1", port=5555, reconnect=False)
                    # wait briefly for a possible reconnect attempt
                    await asyncio.sleep(0.1)
                    expected_items = ("was refused",)
                    for expected_item in expected_items:
                        self.assertTrue(
                            any(expected_item in log_item for log_item in log.output)
                        )

                    unexpected_items = (
                        "Attempting reconnect in",
                        "seconds before connection attempt",
                    )
                    for unexpected_item in unexpected_items:
                        self.assertFalse(
                            any(unexpected_item in log_item for log_item in log.output)
                        )

                    await client_ep.stop()

    async def test_client_server_interaction_without_msg_id(self):
        """ check client server interactions without message identifiers """

        server_on_message_mock = asynctest.CoroutineMock()
        server_on_started_mock = asynctest.CoroutineMock()
        server_on_stopped_mock = asynctest.CoroutineMock()
        server_on_peer_available_mock = asynctest.CoroutineMock()
        server_on_peer_unavailable_mock = asynctest.CoroutineMock()

        server_ep = MtiStreamServer(
            on_message=server_on_message_mock,
            on_started=server_on_started_mock,
            on_stopped=server_on_stopped_mock,
            on_peer_available=server_on_peer_available_mock,
            on_peer_unavailable=server_on_peer_unavailable_mock,
        )

        await server_ep.start(addr="127.0.0.1", family=socket.AF_INET)
        self.assertTrue(server_on_started_mock.called)
        self.assertEqual(
            server_on_started_mock.call_args, unittest.mock.call(server_ep)
        )

        address, port = server_ep.bindings[0]

        client_on_message_mock = asynctest.CoroutineMock()
        client_on_started_mock = asynctest.CoroutineMock()
        client_on_stopped_mock = asynctest.CoroutineMock()
        client_on_peer_available_mock = asynctest.CoroutineMock()
        client_on_peer_unavailable_mock = asynctest.CoroutineMock()

        client_ep = MtiStreamClient(
            on_message=client_on_message_mock,
            on_started=client_on_started_mock,
            on_stopped=client_on_stopped_mock,
            on_peer_available=client_on_peer_available_mock,
            on_peer_unavailable=client_on_peer_unavailable_mock,
        )

        await client_ep.start(addr=address, port=port, family=socket.AF_INET)
        await asyncio.sleep(0.3)

        self.assertTrue(client_on_started_mock.called)
        self.assertEqual(
            client_on_started_mock.call_args, unittest.mock.call(client_ep)
        )

        self.assertTrue(client_on_peer_available_mock.called)
        (args, kwargs) = client_on_peer_available_mock.call_args
        cli, svr_peer_id = args
        self.assertIsInstance(cli, MtiStreamClient)
        self.assertIsInstance(svr_peer_id, bytes)

        self.assertTrue(server_on_peer_available_mock.called)
        (args, kwargs) = server_on_peer_available_mock.call_args
        svr, cli_peer_id = args
        self.assertIsInstance(svr, MtiStreamServer)
        self.assertIsInstance(cli_peer_id, bytes)

        self.assertEqual(len(client_ep.connections), 1)

        # Send a msg from client to server
        sent_msg = b"Hello World"
        client_ep.send(sent_msg)
        await asyncio.sleep(0.1)

        self.assertTrue(server_on_message_mock.called)
        (args, kwargs) = server_on_message_mock.call_args_list[0]
        svr, received_msg = args
        sender_id = kwargs["peer_id"]
        self.assertEqual(svr, server_ep)
        self.assertEqual(received_msg, sent_msg)

        # Send a msg from server to client
        server_ep.send(received_msg, peer_id=sender_id)
        await asyncio.sleep(0.1)
        (args, kwargs) = client_on_message_mock.call_args_list[0]
        cli, received_msg = args
        sender_id = kwargs["peer_id"]
        self.assertEqual(cli, client_ep)
        self.assertEqual(received_msg, sent_msg)

        await client_ep.stop()
        await asyncio.sleep(0.1)

        self.assertTrue(client_on_stopped_mock.called)
        self.assertEqual(
            client_on_stopped_mock.call_args, unittest.mock.call(client_ep)
        )

        self.assertTrue(client_on_peer_unavailable_mock.called)
        self.assertTrue(server_on_peer_unavailable_mock.called)

        await server_ep.stop()
        self.assertTrue(server_on_stopped_mock.called)
        self.assertEqual(
            server_on_stopped_mock.call_args, unittest.mock.call(server_ep)
        )

    async def test_json_client_server_interaction_without_msg_id(self):
        """ check JSON client server interactions without message identifiers """

        server_on_message_mock = asynctest.CoroutineMock()
        server_on_started_mock = asynctest.CoroutineMock()
        server_on_stopped_mock = asynctest.CoroutineMock()
        server_on_peer_available_mock = asynctest.CoroutineMock()
        server_on_peer_unavailable_mock = asynctest.CoroutineMock()

        server_ep = MtiStreamServer(
            on_message=server_on_message_mock,
            on_started=server_on_started_mock,
            on_stopped=server_on_stopped_mock,
            on_peer_available=server_on_peer_available_mock,
            on_peer_unavailable=server_on_peer_unavailable_mock,
            content_type=serialization.CONTENT_TYPE_JSON,
        )

        await server_ep.start(addr="127.0.0.1", family=socket.AF_INET)
        self.assertTrue(server_on_started_mock.called)

        address, port = server_ep.bindings[0]

        client_on_message_mock = asynctest.CoroutineMock()
        client_on_started_mock = asynctest.CoroutineMock()
        client_on_stopped_mock = asynctest.CoroutineMock()
        client_on_peer_available_mock = asynctest.CoroutineMock()
        client_on_peer_unavailable_mock = asynctest.CoroutineMock()

        client_ep = MtiStreamClient(
            on_message=client_on_message_mock,
            on_started=client_on_started_mock,
            on_stopped=client_on_stopped_mock,
            on_peer_available=client_on_peer_available_mock,
            on_peer_unavailable=client_on_peer_unavailable_mock,
            content_type=serialization.CONTENT_TYPE_JSON,
        )

        await client_ep.start(addr=address, port=port, family=socket.AF_INET)
        await asyncio.sleep(0.3)

        self.assertTrue(client_on_started_mock.called)
        self.assertTrue(client_on_peer_available_mock.called)
        self.assertTrue(server_on_peer_available_mock.called)

        self.assertEqual(len(client_ep.connections), 1)

        test_msg_in = dict(latitude=130.0, longitude=-30.0, altitude=50.0)

        # Send a msg with identifier from client to server
        client_ep.send(test_msg_in)
        await asyncio.sleep(0.1)

        self.assertTrue(server_on_message_mock.called)
        (args, kwargs) = server_on_message_mock.call_args_list[0]
        svr, received_msg = args
        sender_id = kwargs["peer_id"]
        self.assertEqual(received_msg, test_msg_in)

        # Send a msg from server to client
        server_ep.send(received_msg, peer_id=sender_id)
        await asyncio.sleep(0.1)
        (args, kwargs) = client_on_message_mock.call_args_list[0]
        cli, received_msg = args
        sender_id = kwargs["peer_id"]
        self.assertEqual(received_msg, test_msg_in)

        await client_ep.stop()
        await asyncio.sleep(0.1)

        self.assertTrue(client_on_stopped_mock.called)
        self.assertTrue(client_on_peer_unavailable_mock.called)
        self.assertTrue(server_on_peer_unavailable_mock.called)

        await server_ep.stop()
        self.assertTrue(server_on_stopped_mock.called)

    @unittest.skipUnless(serialization.have_msgpack, "requires msgpack")
    async def test_msgpack_client_server_interaction_without_msg_id(self):
        """ check msgpack client server interactions without message identifiers """

        server_on_message_mock = asynctest.CoroutineMock()
        server_on_started_mock = asynctest.CoroutineMock()
        server_on_stopped_mock = asynctest.CoroutineMock()
        server_on_peer_available_mock = asynctest.CoroutineMock()
        server_on_peer_unavailable_mock = asynctest.CoroutineMock()

        server_ep = MtiStreamServer(
            on_message=server_on_message_mock,
            on_started=server_on_started_mock,
            on_stopped=server_on_stopped_mock,
            on_peer_available=server_on_peer_available_mock,
            on_peer_unavailable=server_on_peer_unavailable_mock,
            content_type=serialization.CONTENT_TYPE_MSGPACK,
        )

        await server_ep.start(addr="127.0.0.1", family=socket.AF_INET)
        self.assertTrue(server_on_started_mock.called)

        address, port = server_ep.bindings[0]

        client_on_message_mock = asynctest.CoroutineMock()
        client_on_started_mock = asynctest.CoroutineMock()
        client_on_stopped_mock = asynctest.CoroutineMock()
        client_on_peer_available_mock = asynctest.CoroutineMock()
        client_on_peer_unavailable_mock = asynctest.CoroutineMock()

        client_ep = MtiStreamClient(
            on_message=client_on_message_mock,
            on_started=client_on_started_mock,
            on_stopped=client_on_stopped_mock,
            on_peer_available=client_on_peer_available_mock,
            on_peer_unavailable=client_on_peer_unavailable_mock,
            content_type=serialization.CONTENT_TYPE_MSGPACK,
        )

        await client_ep.start(addr=address, port=port, family=socket.AF_INET)
        await asyncio.sleep(0.3)

        self.assertTrue(client_on_started_mock.called)
        self.assertTrue(client_on_peer_available_mock.called)
        self.assertTrue(server_on_peer_available_mock.called)

        self.assertEqual(len(client_ep.connections), 1)

        test_msg_in = dict(latitude=130.0, longitude=-30.0, altitude=50.0)

        # Send a msg with identifier from client to server
        client_ep.send(test_msg_in)
        await asyncio.sleep(0.1)

        self.assertTrue(server_on_message_mock.called)
        (args, kwargs) = server_on_message_mock.call_args_list[0]
        svr, received_msg = args
        sender_id = kwargs["peer_id"]
        self.assertEqual(received_msg, test_msg_in)

        # Send a msg from server to client
        server_ep.send(received_msg, peer_id=sender_id)
        await asyncio.sleep(0.1)
        (args, kwargs) = client_on_message_mock.call_args_list[0]
        cli, received_msg = args
        sender_id = kwargs["peer_id"]
        self.assertEqual(received_msg, test_msg_in)

        await client_ep.stop()
        await asyncio.sleep(0.1)

        self.assertTrue(client_on_stopped_mock.called)
        self.assertTrue(client_on_peer_unavailable_mock.called)
        self.assertTrue(server_on_peer_unavailable_mock.called)

        await server_ep.stop()
        self.assertTrue(server_on_stopped_mock.called)

    @unittest.skipUnless(serialization.have_protobuf, "requires google protobuf")
    async def test_protobuf_client_server_interaction_with_msg_id(self):
        """ check protobuf client server interactions with message identifiers """
        from position_pb2 import Position

        protobuf_data = Position(
            latitude=130.0, longitude=-30.0, altitude=50.0, status=Position.SIMULATED
        )

        server_on_message_mock = asynctest.CoroutineMock()
        server_on_started_mock = asynctest.CoroutineMock()
        server_on_stopped_mock = asynctest.CoroutineMock()
        server_on_peer_available_mock = asynctest.CoroutineMock()
        server_on_peer_unavailable_mock = asynctest.CoroutineMock()

        server_ep = MtiStreamServer(
            on_message=server_on_message_mock,
            on_started=server_on_started_mock,
            on_stopped=server_on_stopped_mock,
            on_peer_available=server_on_peer_available_mock,
            on_peer_unavailable=server_on_peer_unavailable_mock,
            content_type=serialization.CONTENT_TYPE_PROTOBUF,
        )

        await server_ep.start(addr="127.0.0.1", family=socket.AF_INET)
        self.assertTrue(server_on_started_mock.called)

        address, port = server_ep.bindings[0]

        client_on_message_mock = asynctest.CoroutineMock()
        client_on_started_mock = asynctest.CoroutineMock()
        client_on_stopped_mock = asynctest.CoroutineMock()
        client_on_peer_available_mock = asynctest.CoroutineMock()
        client_on_peer_unavailable_mock = asynctest.CoroutineMock()

        client_ep = MtiStreamClient(
            on_message=client_on_message_mock,
            on_started=client_on_started_mock,
            on_stopped=client_on_stopped_mock,
            on_peer_available=client_on_peer_available_mock,
            on_peer_unavailable=client_on_peer_unavailable_mock,
            content_type=serialization.CONTENT_TYPE_PROTOBUF,
        )

        await client_ep.start(addr=address, port=port, family=socket.AF_INET)
        await asyncio.sleep(0.3)

        self.assertTrue(client_on_started_mock.called)
        self.assertTrue(client_on_peer_available_mock.called)
        self.assertTrue(server_on_peer_available_mock.called)

        self.assertEqual(len(client_ep.connections), 1)

        # Register a message object with a unique message identifier. Only
        # one of these calls is actually required to register the identifier
        # with the object because a common serializer is used in this test
        # case where both the client and server are instantiated.
        # However, both calls are made to be more representative of how
        # messages would be registered in a real application.
        type_identifier = 1
        server_ep.register_message(type_identifier, Position)
        client_ep.register_message(type_identifier, Position)

        test_msg_in = protobuf_data

        # Send a msg with identifier from client to server
        client_ep.send(test_msg_in, type_identifier=type_identifier)
        await asyncio.sleep(0.1)

        self.assertTrue(server_on_message_mock.called)
        (args, kwargs) = server_on_message_mock.call_args_list[0]
        svr, received_msg = args
        received_msg_id = kwargs["type_identifier"]
        sender_id = kwargs["peer_id"]
        self.assertEqual(received_msg, test_msg_in)
        self.assertEqual(received_msg_id, type_identifier)

        # Send a msg from server to client
        server_ep.send(received_msg, type_identifier=received_msg_id, peer_id=sender_id)
        await asyncio.sleep(0.1)
        (args, kwargs) = client_on_message_mock.call_args_list[0]
        cli, received_msg = args
        received_msg_id = kwargs["type_identifier"]
        sender_id = kwargs["peer_id"]
        self.assertEqual(received_msg, test_msg_in)
        self.assertEqual(received_msg_id, type_identifier)

        await client_ep.stop()
        await asyncio.sleep(0.1)

        self.assertTrue(client_on_stopped_mock.called)
        self.assertTrue(client_on_peer_unavailable_mock.called)
        self.assertTrue(server_on_peer_unavailable_mock.called)

        await server_ep.stop()
        self.assertTrue(server_on_stopped_mock.called)
