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

        await listener_ep.start()
        self.assertTrue(listener_on_started_mock.called)
        self.assertTrue(listener_on_peer_available_mock.called)

        address, port = listener_ep.bindings[0]

        # Check that starting a server that is already started does not
        # have any consequences
        with self.assertLogs(
            "gestalt.comms.datagram.endpoint", level=logging.WARN
        ) as log:
            await listener_ep.start()
            expected_items = ("is already started",)
            for expected_item in expected_items:
                self.assertTrue(
                    any(expected_item in log_item for log_item in log.output)
                )

        await listener_ep.stop()
        self.assertTrue(listener_on_stopped_mock.called)

        # Check that stopping a server that is already stopped does not
        # have any consequences
        with self.assertLogs(
            "gestalt.comms.datagram.endpoint", level=logging.WARN
        ) as log:
            await listener_ep.stop()
            expected_items = ("is already stopped",)
            for expected_item in expected_items:
                self.assertTrue(
                    any(expected_item in log_item for log_item in log.output)
                )

    # async def test_start_server_on_unavailable_port(self):
    #     """ check starting server on a used port raises an exception """
    #     # Occupy a port by starting a server
    #     listener = await self.loop.create_server(
    #         asyncio.Protocol, host="", port=0, family=socket.AF_INET
    #     )
    #     host, occupied_port = listener.sockets[0].getsockname()

    #     print(host, occupied_port)
    #     try:
    #         listener_on_message_mock = unittest.mock.Mock()
    #         listener_on_started_mock = unittest.mock.Mock()
    #         listener_on_stopped_mock = unittest.mock.Mock()
    #         listener_on_peer_available_mock = unittest.mock.Mock()
    #         listener_on_peer_unavailable_mock = unittest.mock.Mock()

    #         listener_ep = NetstringDatagramEndpoint(
    #             on_message=listener_on_message_mock,
    #             on_started=listener_on_started_mock,
    #             on_stopped=listener_on_stopped_mock,
    #             on_peer_available=listener_on_peer_available_mock,
    #             on_peer_unavailable=listener_on_peer_unavailable_mock,
    #         )

    #         with self.assertLogs(
    #             "gestalt.comms.datagram.endpoint", level=logging.ERROR
    #         ) as log:
    #             with self.assertRaises(Exception):
    #                 await listener_ep.start(addr=host, port=occupied_port)

    #         self.assertFalse(listener_on_started_mock.called)

    #         await listener_ep.stop()
    #         self.assertTrue(listener_on_stopped_mock.called)
    #     finally:
    #         listener.close()
    #         await listener.wait_closed()

    # async def test_client_server_interaction(self):
    #     """ check client server interactions """

    #     listener_on_message_mock = asynctest.CoroutineMock()
    #     listener_on_started_mock = asynctest.CoroutineMock()
    #     listener_on_stopped_mock = asynctest.CoroutineMock()
    #     listener_on_peer_available_mock = asynctest.CoroutineMock()
    #     listener_on_peer_unavailable_mock = asynctest.CoroutineMock()

    #     listener_ep = NetstringDatagramEndpoint(
    #         on_message=listener_on_message_mock,
    #         on_started=listener_on_started_mock,
    #         on_stopped=listener_on_stopped_mock,
    #         on_peer_available=listener_on_peer_available_mock,
    #         on_peer_unavailable=listener_on_peer_unavailable_mock,
    #     )

    #     await listener_ep.start(addr="127.0.0.1")
    #     self.assertTrue(listener_on_started_mock.called)

    #     address, port = listener_ep.bindings[0]

    #     client_on_message_mock = asynctest.CoroutineMock()
    #     client_on_started_mock = asynctest.CoroutineMock()
    #     client_on_stopped_mock = asynctest.CoroutineMock()
    #     client_on_peer_available_mock = asynctest.CoroutineMock()
    #     client_on_peer_unavailable_mock = asynctest.CoroutineMock()

    #     client_ep = NetstringDatagramEndpoint(
    #         on_message=client_on_message_mock,
    #         on_started=client_on_started_mock,
    #         on_stopped=client_on_stopped_mock,
    #         on_peer_available=client_on_peer_available_mock,
    #         on_peer_unavailable=client_on_peer_unavailable_mock,
    #     )

    #     await client_ep.start(addr=address, port=port, remote=True)
    #     await asyncio.sleep(0.3)

    #     self.assertTrue(client_on_started_mock.called)
    #     self.assertTrue(client_on_peer_available_mock.called)
    #     self.assertTrue(listener_on_peer_available_mock.called)

    #     # Send a msg from client to listener
    #     sent_msg = b"Hello World"
    #     client_ep.send(sent_msg)
    #     await asyncio.sleep(0.1)

    #     self.assertTrue(listener_on_message_mock.called)
    #     (args, kwargs) = listener_on_message_mock.call_args_list[0]
    #     received_msg = args[0]
    #     sender_id = kwargs["peer_id"]
    #     self.assertEqual(received_msg, sent_msg)

    #     # Send a msg from listener to client
    #     listener_ep.send(received_msg, peer_id=sender_id)
    #     await asyncio.sleep(0.1)
    #     (args, kwargs) = client_on_message_mock.call_args_list[0]
    #     received_msg = args[0]
    #     sender_id = kwargs["peer_id"]
    #     self.assertEqual(received_msg, sent_msg)

    #     await client_ep.stop()
    #     await asyncio.sleep(0.1)

    #     self.assertTrue(client_on_stopped_mock.called)
    #     self.assertTrue(client_on_peer_unavailable_mock.called)
    #     self.assertTrue(listener_on_peer_unavailable_mock.called)

    #     await listener_ep.stop()
    #     self.assertTrue(listener_on_stopped_mock.called)
