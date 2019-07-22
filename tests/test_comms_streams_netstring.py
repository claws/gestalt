import asyncio
import asynctest
import logging
import pathlib
import socket
import ssl
import sys
import unittest.mock
from gestalt.comms.stream.netstring import NetstringStreamClient, NetstringStreamServer
import tls_utils

py_ver = sys.version_info
PY36 = py_ver.major == 3 and py_ver.minor == 6
PY37 = py_ver.major == 3 and py_ver.minor == 7


class NetstringStreamEndpointTestCase(asynctest.TestCase):
    async def test_start_server(self):
        server_on_message_mock = unittest.mock.Mock()
        server_on_started_mock = unittest.mock.Mock()
        server_on_stopped_mock = unittest.mock.Mock()
        server_on_peer_available_mock = unittest.mock.Mock()
        server_on_peer_unavailable_mock = unittest.mock.Mock()

        server_ep = NetstringStreamServer(
            on_message=server_on_message_mock,
            on_started=server_on_started_mock,
            on_stopped=server_on_stopped_mock,
            on_peer_available=server_on_peer_available_mock,
            on_peer_unavailable=server_on_peer_unavailable_mock,
        )

        await server_ep.start()
        self.assertTrue(server_on_started_mock.called)

        address, port = server_ep.bindings[0]

        # Check that starting a server that is already started does not
        # have any consequences
        await server_ep.start()

        await server_ep.stop()
        self.assertTrue(server_on_stopped_mock.called)

        # Check that stopping a server that is already stopped does not
        # have any consequences
        await server_ep.stop()

    async def test_start_server_on_unavailable_port(self):
        """ check starting server on a used port raises an exception """
        # Occupy a port by starting a server
        listener = await self.loop.create_server(
            asyncio.Protocol, host="", port=0, family=socket.AF_INET
        )
        host, occupied_port = listener.sockets[0].getsockname()

        try:
            server_on_message_mock = unittest.mock.Mock()
            server_on_started_mock = unittest.mock.Mock()
            server_on_stopped_mock = unittest.mock.Mock()
            server_on_peer_available_mock = unittest.mock.Mock()
            server_on_peer_unavailable_mock = unittest.mock.Mock()

            server_ep = NetstringStreamServer(
                on_message=server_on_message_mock,
                on_started=server_on_started_mock,
                on_stopped=server_on_stopped_mock,
                on_peer_available=server_on_peer_available_mock,
                on_peer_unavailable=server_on_peer_unavailable_mock,
            )

            with self.assertLogs(
                "gestalt.comms.stream.endpoint", level=logging.ERROR
            ) as log:
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

        client_ep = NetstringStreamClient(
            on_started=client_on_started_mock, on_stopped=client_on_stopped_mock
        )

        sub_tests = (
            # addr_value, description
            ("localhost", "Using localhost as address"),
            ("127.0.0.1", "Using 127.0.0.1 as address"),
        )

        # Attempt connection with reconnect=True
        for addr_value, subtest_description in sub_tests:
            with self.subTest(subtest_description):
                with self.assertLogs(
                    "gestalt.comms.stream.endpoint", level=logging.INFO
                ) as log:
                    await client_ep.start(addr=addr_value, port=5555)
                    # wait briefly for a reconnect attempt
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
                    "gestalt.comms.stream.endpoint", level=logging.DEBUG
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
                    for expected_item in unexpected_items:
                        self.assertFalse(
                            any(expected_item in log_item for log_item in log.output)
                        )

                    await client_ep.stop()

    async def test_client_server_interaction(self):
        """ check client server interactions """

        server_on_message_mock = asynctest.CoroutineMock()
        server_on_started_mock = asynctest.CoroutineMock()
        server_on_stopped_mock = asynctest.CoroutineMock()
        server_on_peer_available_mock = asynctest.CoroutineMock()
        server_on_peer_unavailable_mock = asynctest.CoroutineMock()

        server_ep = NetstringStreamServer(
            on_message=server_on_message_mock,
            on_started=server_on_started_mock,
            on_stopped=server_on_stopped_mock,
            on_peer_available=server_on_peer_available_mock,
            on_peer_unavailable=server_on_peer_unavailable_mock,
        )

        await server_ep.start(addr="127.0.0.1", family=socket.AF_INET)
        self.assertTrue(server_on_started_mock.called)

        address, port = server_ep.bindings[0]

        client_on_message_mock = asynctest.CoroutineMock()
        client_on_started_mock = asynctest.CoroutineMock()
        client_on_stopped_mock = asynctest.CoroutineMock()
        client_on_peer_available_mock = asynctest.CoroutineMock()
        client_on_peer_unavailable_mock = asynctest.CoroutineMock()

        client_ep = NetstringStreamClient(
            on_message=client_on_message_mock,
            on_started=client_on_started_mock,
            on_stopped=client_on_stopped_mock,
            on_peer_available=client_on_peer_available_mock,
            on_peer_unavailable=client_on_peer_unavailable_mock,
        )

        await client_ep.start(addr=address, port=port, family=socket.AF_INET)
        await asyncio.sleep(0.3)

        self.assertTrue(client_on_started_mock.called)
        self.assertTrue(client_on_peer_available_mock.called)
        self.assertTrue(server_on_peer_available_mock.called)

        # Send a msg from client to server
        sent_msg = b"Hello World"
        client_ep.send(sent_msg)
        await asyncio.sleep(0.1)

        self.assertTrue(server_on_message_mock.called)
        (args, kwargs) = server_on_message_mock.call_args_list[0]
        received_msg = args[0]
        sender_id = kwargs["peer_id"]
        self.assertEqual(received_msg, sent_msg)

        # Send a msg from server to client
        server_ep.send(received_msg, peer_id=sender_id)
        await asyncio.sleep(0.1)
        (args, kwargs) = client_on_message_mock.call_args_list[0]
        received_msg = args[0]
        sender_id = kwargs["peer_id"]
        self.assertEqual(received_msg, sent_msg)

        await client_ep.stop()
        await asyncio.sleep(0.1)

        self.assertTrue(client_on_stopped_mock.called)
        self.assertTrue(client_on_peer_unavailable_mock.called)
        self.assertTrue(server_on_peer_unavailable_mock.called)

        await server_ep.stop()
        self.assertTrue(server_on_stopped_mock.called)

    @unittest.skipUnless(tls_utils.CERTS_EXIST, "cert files do not exist")
    async def test_client_server_ssl_without_client_access_to_root_ca(self):
        """ check client correctly fails to verify server when no root CA is provided """
        certificates = tls_utils.get_certs()

        server_certs = (
            certificates.ca_cert,
            certificates.server_cert,
            certificates.server_key,
        )
        server_ctx = tls_utils.create_ssl_server_context(*server_certs)

        # Create a client context where the root CA is not loaded. This should
        # prevent client from authenticating server.
        client_ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
        client_ctx.set_ciphers("ECDH+AESGCM")
        client_ctx.load_cert_chain(
            certfile=certificates.client_cert, keyfile=certificates.client_key
        )
        # client_ctx.load_verify_locations(cafile=certificates.ca_cert)
        client_ctx.check_hostname = True
        client_ctx.options |= (
            ssl.PROTOCOL_TLSv1_1 | ssl.OP_NO_COMPRESSION  # selects older version
        )

        server_on_started_mock = asynctest.CoroutineMock()
        server_on_stopped_mock = asynctest.CoroutineMock()

        server_ep = NetstringStreamServer(
            on_started=server_on_started_mock, on_stopped=server_on_stopped_mock
        )

        await server_ep.start(addr="127.0.0.1", family=socket.AF_INET, ssl=server_ctx)
        self.assertTrue(server_on_started_mock.called)

        address, port = server_ep.bindings[0]

        client_ep = NetstringStreamClient()

        try:
            if PY36:
                with self.assertLogs(
                    "gestalt.comms.stream.endpoint", level=logging.ERROR
                ) as log:
                    await client_ep.start(
                        addr=address,
                        port=port,
                        family=socket.AF_INET,
                        ssl=client_ctx,
                        reconnect=False,
                    )
                    await asyncio.sleep(0.1)

                    expected_items = ("was refused", "certificate verify failed")
                    for expected_item in expected_items:
                        self.assertTrue(
                            any(expected_item in log_item for log_item in log.output)
                        )
            else:
                # suppress ssl exception traceback logs reporting
                # "certificate verify failed: self signed certificate in certificate chain"
                with self.assertLogs(level=logging.ERROR) as root_log:
                    with self.assertLogs(
                        "gestalt.comms.stream.endpoint", level=logging.ERROR
                    ) as log:
                        await client_ep.start(
                            addr=address,
                            port=port,
                            family=socket.AF_INET,
                            ssl=client_ctx,
                            reconnect=False,
                        )
                        await asyncio.sleep(0.1)

                        expected_items = ("was refused", "certificate verify failed")
                        for expected_item in expected_items:
                            self.assertTrue(
                                any(
                                    expected_item in log_item for log_item in log.output
                                )
                            )

                        # In Python 3.7+ Errors get sent to root logger
                        expected_items = (
                            "certificate verify failed: self signed certificate in certificate chain",
                        )
                        for expected_item in expected_items:
                            self.assertTrue(
                                any(
                                    expected_item in log_item
                                    for log_item in root_log.output
                                )
                            )

        finally:
            await client_ep.stop()
            await asyncio.sleep(0.1)

            await server_ep.stop()
            self.assertTrue(server_on_stopped_mock.called)

    @unittest.skipUnless(tls_utils.CERTS_EXIST, "cert files do not exist")
    async def test_client_server_ssl_without_client_certificates(self):
        """ check server correct fails to verify client not supplying a certificate """
        certificates = tls_utils.get_certs()

        server_certs = (
            certificates.ca_cert,
            certificates.server_cert,
            certificates.server_key,
        )
        server_ctx = tls_utils.create_ssl_server_context(*server_certs)

        # Create a client context where the root CA is not loaded. This should
        # prevent client from authenticating server.
        client_ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
        client_ctx.set_ciphers("ECDH+AESGCM")
        # client_ctx.load_cert_chain(
        #    certfile=certificates.client_cert, keyfile=certificates.client_key)
        client_ctx.load_verify_locations(cafile=certificates.ca_cert)
        client_ctx.check_hostname = True
        client_ctx.options |= (
            ssl.PROTOCOL_TLS
            | ssl.OP_NO_TLSv1  # This selects the highest support version
            | ssl.OP_NO_TLSv1_1
            | ssl.OP_NO_COMPRESSION
        )

        server_on_started_mock = asynctest.CoroutineMock()
        server_on_stopped_mock = asynctest.CoroutineMock()

        server_ep = NetstringStreamServer(
            on_started=server_on_started_mock, on_stopped=server_on_stopped_mock
        )

        await server_ep.start(addr="127.0.0.1", family=socket.AF_INET, ssl=server_ctx)
        self.assertTrue(server_on_started_mock.called)

        address, port = server_ep.bindings[0]

        client_ep = NetstringStreamClient()

        try:
            if PY36:
                with self.assertLogs(
                    "gestalt.comms.stream.endpoint", level=logging.ERROR
                ) as log:
                    await client_ep.start(
                        addr=address,
                        port=port,
                        family=socket.AF_INET,
                        ssl=client_ctx,
                        reconnect=False,
                    )
                    await asyncio.sleep(0.1)

                    expected_items = ("was refused",)
                    for expected_item in expected_items:
                        self.assertTrue(
                            any(expected_item in log_item for log_item in log.output)
                        )

            else:
                with self.assertLogs(level=logging.ERROR) as root_log:
                    with self.assertLogs(
                        "gestalt.comms.stream.endpoint", level=logging.ERROR
                    ) as log:
                        await client_ep.start(
                            addr=address,
                            port=port,
                            family=socket.AF_INET,
                            ssl=client_ctx,
                            reconnect=False,
                        )
                        await asyncio.sleep(0.1)

                        expected_items = ("was refused",)
                        for expected_item in expected_items:
                            self.assertTrue(
                                any(
                                    expected_item in log_item for log_item in log.output
                                )
                            )

                        # In Python 3.7+ Errors get sent to root logger
                        expected_items = ("peer did not return a certificate",)
                        for expected_item in expected_items:
                            self.assertTrue(
                                any(
                                    expected_item in log_item
                                    for log_item in root_log.output
                                )
                            )
        finally:
            await client_ep.stop()
            await asyncio.sleep(0.1)

            await server_ep.stop()
            self.assertTrue(server_on_stopped_mock.called)

    @unittest.skipUnless(tls_utils.CERTS_EXIST, "cert files do not exist")
    async def test_client_server_ssl_with_selfsigned_client_certificates(self):
        """ check server correctly fails to verify client not supplying a certificate """
        certificates = tls_utils.get_certs()

        server_certs = (
            certificates.ca_cert,
            certificates.server_cert,
            certificates.server_key,
        )
        server_ctx = tls_utils.create_ssl_server_context(*server_certs)

        # Create a client context where the root CA is not loaded. This should
        # prevent client from authenticating server.
        client_ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
        client_ctx.set_ciphers("ECDH+AESGCM")
        # Load client certs that were self signed (not signed by root CA)
        client_cert = certificates.client_cert.replace("client", "client2")
        client_key = certificates.client_key.replace("client", "client2")
        client_ctx.load_cert_chain(certfile=client_cert, keyfile=client_key)
        client_ctx.load_verify_locations(cafile=certificates.ca_cert)
        client_ctx.check_hostname = True
        client_ctx.options |= (
            ssl.PROTOCOL_TLS
            | ssl.OP_NO_TLSv1  # This selects the highest support version
            | ssl.OP_NO_TLSv1_1
            | ssl.OP_NO_COMPRESSION
        )

        server_on_started_mock = asynctest.CoroutineMock()
        server_on_stopped_mock = asynctest.CoroutineMock()

        server_ep = NetstringStreamServer(
            on_started=server_on_started_mock, on_stopped=server_on_stopped_mock
        )

        await server_ep.start(addr="127.0.0.1", family=socket.AF_INET, ssl=server_ctx)
        self.assertTrue(server_on_started_mock.called)

        address, port = server_ep.bindings[0]

        client_ep = NetstringStreamClient()

        try:
            if PY36:
                with self.assertLogs(
                    "gestalt.comms.stream.endpoint", level=logging.ERROR
                ) as log:
                    await client_ep.start(
                        addr=address, port=port, family=socket.AF_INET, ssl=client_ctx
                    )
                    await asyncio.sleep(0.1)

                    expected_items = ("was refused",)
                    for expected_item in expected_items:
                        self.assertTrue(
                            any(expected_item in log_item for log_item in log.output)
                        )
            else:

                with self.assertLogs(level=logging.ERROR) as root_log:
                    with self.assertLogs(
                        "gestalt.comms.stream.endpoint", level=logging.ERROR
                    ) as log:
                        await client_ep.start(
                            addr=address,
                            port=port,
                            family=socket.AF_INET,
                            ssl=client_ctx,
                        )
                        await asyncio.sleep(0.1)

                        expected_items = ("was refused",)
                        for expected_item in expected_items:
                            self.assertTrue(
                                any(
                                    expected_item in log_item for log_item in log.output
                                )
                            )

                        # In Python 3.7+ Errors get sent to root logger
                        # Errors reported to root logger are possibly related to:
                        # https://stackoverflow.com/questions/52012488/ssl-asyncio-traceback-even-when-error-is-handled
                        expected_items = (
                            "certificate verify failed: self signed certificate",
                        )
                        for expected_item in expected_items:
                            self.assertTrue(
                                any(
                                    expected_item in log_item
                                    for log_item in root_log.output
                                )
                            )

        finally:
            await client_ep.stop()
            await asyncio.sleep(0.1)

            await server_ep.stop()
            self.assertTrue(server_on_stopped_mock.called)
