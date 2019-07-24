import asyncio
import asynctest
import logging
import socket
import unittest.mock
from gestalt import serialization
from gestalt.comms.datagram.netstring import NetstringDatagramEndpoint


class NetstringDatagramEndpointTestCase(asynctest.TestCase):
    async def test_start_server(self):
        listener_on_message_mock = unittest.mock.Mock()
        listener_on_started_mock = unittest.mock.Mock()
        listener_on_stopped_mock = unittest.mock.Mock()
        listener_on_peer_available_mock = unittest.mock.Mock()
        listener_on_peer_unavailable_mock = unittest.mock.Mock()

        listener_ep = NetstringDatagramEndpoint(
            on_message=listener_on_message_mock,
            on_started=listener_on_started_mock,
            on_stopped=listener_on_stopped_mock,
            on_peer_available=listener_on_peer_available_mock,
            on_peer_unavailable=listener_on_peer_unavailable_mock,
        )

        # Expect an exception if niether local_addr or remote addr are
        # specified
        with self.assertRaises(Exception) as cm:
            await listener_ep.start()
        expected = "At least one of local_addr or remote addr must be defined"
        self.assertIn(expected, str(cm.exception))

        await listener_ep.start(local_addr=("0.0.0.0", 0))
        self.assertTrue(listener_on_started_mock.called)
        self.assertTrue(listener_on_peer_available_mock.called)

        address, port = listener_ep.bindings[0]

        # Check that starting a server that is already started does not
        # have any consequences
        await listener_ep.start()

        await listener_ep.stop()
        self.assertTrue(listener_on_stopped_mock.called)

        # Check that stopping a server that is already stopped does not
        # have any consequences
        await listener_ep.stop()

    async def test_start_server_on_unavailable_port(self):
        """ check starting server on a used port raises an exception """
        # Occupy a port by starting a UDP endpoint on it first.
        first_ep = NetstringDatagramEndpoint()
        await first_ep.start(local_addr=("0.0.0.0", 0))
        host, occupied_port = first_ep.bindings[0]

        try:
            listener_on_message_mock = unittest.mock.Mock()
            listener_on_started_mock = unittest.mock.Mock()
            listener_on_stopped_mock = unittest.mock.Mock()
            listener_on_peer_available_mock = unittest.mock.Mock()
            listener_on_peer_unavailable_mock = unittest.mock.Mock()

            listener_ep = NetstringDatagramEndpoint(
                on_message=listener_on_message_mock,
                on_started=listener_on_started_mock,
                on_stopped=listener_on_stopped_mock,
                on_peer_available=listener_on_peer_available_mock,
                on_peer_unavailable=listener_on_peer_unavailable_mock,
            )

            with self.assertLogs(
                "gestalt.comms.datagram.endpoint", level=logging.ERROR
            ) as log:
                with self.assertRaises(Exception):
                    await listener_ep.start(local_addr=(host, occupied_port))
                    address, port = listener_ep.bindings[0]

            self.assertFalse(listener_on_started_mock.called)

            await listener_ep.stop()
            # endpoint never actually started so it should not really need
            # to be stopped.
            self.assertFalse(listener_on_stopped_mock.called)
        finally:
            await first_ep.stop()

    async def test_client_server_interaction(self):
        """ check client server interactions """

        listener_on_message_mock = asynctest.CoroutineMock()
        listener_on_started_mock = asynctest.CoroutineMock()
        listener_on_stopped_mock = asynctest.CoroutineMock()
        listener_on_peer_available_mock = asynctest.CoroutineMock()
        listener_on_peer_unavailable_mock = asynctest.CoroutineMock()

        listener_ep = NetstringDatagramEndpoint(
            on_message=listener_on_message_mock,
            on_started=listener_on_started_mock,
            on_stopped=listener_on_stopped_mock,
            on_peer_available=listener_on_peer_available_mock,
            on_peer_unavailable=listener_on_peer_unavailable_mock,
        )

        await listener_ep.start(local_addr=("127.0.0.1", 0))
        self.assertTrue(listener_on_started_mock.called)

        address, port = listener_ep.bindings[0]

        client_on_message_mock = asynctest.CoroutineMock()
        client_on_started_mock = asynctest.CoroutineMock()
        client_on_stopped_mock = asynctest.CoroutineMock()
        client_on_peer_available_mock = asynctest.CoroutineMock()
        client_on_peer_unavailable_mock = asynctest.CoroutineMock()

        client_ep = NetstringDatagramEndpoint(
            on_message=client_on_message_mock,
            on_started=client_on_started_mock,
            on_stopped=client_on_stopped_mock,
            on_peer_available=client_on_peer_available_mock,
            on_peer_unavailable=client_on_peer_unavailable_mock,
        )

        await client_ep.start(remote_addr=(address, port))
        await asyncio.sleep(0.3)

        self.assertTrue(client_on_started_mock.called)
        self.assertTrue(client_on_peer_available_mock.called)
        self.assertTrue(listener_on_peer_available_mock.called)

        # Send a msg from client to listener
        sent_msg = b"Hello World"
        client_ep.send(sent_msg)
        await asyncio.sleep(0.1)

        self.assertTrue(listener_on_message_mock.called)
        (args, kwargs) = listener_on_message_mock.call_args_list[0]
        ep, received_msg = args
        sender_id = kwargs["peer_id"]
        self.assertIsInstance(ep, NetstringDatagramEndpoint)
        self.assertEqual(received_msg, sent_msg)

        await client_ep.stop()
        await asyncio.sleep(0.1)

        self.assertTrue(client_on_stopped_mock.called)
        self.assertTrue(client_on_peer_unavailable_mock.called)

        await listener_ep.stop()
        self.assertTrue(listener_on_stopped_mock.called)
        self.assertTrue(listener_on_peer_unavailable_mock.called)
