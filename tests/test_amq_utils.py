import json
import unittest
import unittest.mock
from gestalt import compression
from gestalt import serialization
from gestalt.amq import utils


class RabbitmqUtilitiesTestCase(unittest.TestCase):
    def test_encode_payload_exceptions(self):
        TEXT_DATA = "The Quick Brown Fox Jumps Over The Lazy Dog"

        with unittest.mock.patch("gestalt.amq.utils.dumps") as mock_dumps:
            mock_dumps.side_effect = Exception("Boom!")
            with self.assertRaises(Exception):
                headers = {}
                payload, content_type, content_encoding = utils.encode_payload(
                    TEXT_DATA,
                    content_type=serialization.CONTENT_TYPE_TEXT,
                    compression=compression.COMPRESSION_ZLIB,
                    headers=headers,
                )

        with unittest.mock.patch("gestalt.amq.utils.compress") as mock_compress:
            mock_compress.side_effect = Exception("Boom!")
            with self.assertRaises(Exception):
                headers = {}
                payload, content_type, content_encoding = utils.encode_payload(
                    TEXT_DATA,
                    content_type=serialization.CONTENT_TYPE_TEXT,
                    compression=compression.COMPRESSION_ZLIB,
                    headers=headers,
                )

    def test_decode_payload_exceptions(self):
        TEXT_DATA = "The Quick Brown Fox Jumps Over The Lazy Dog"
        compression_mime_type = compression.COMPRESSION_ZLIB
        headers = {}
        payload, content_type, content_encoding = utils.encode_payload(
            TEXT_DATA,
            content_type=serialization.CONTENT_TYPE_TEXT,
            compression=compression_mime_type,
            headers=headers,
        )

        with unittest.mock.patch("gestalt.amq.utils.decompress") as mock_decompress:
            mock_decompress.side_effect = Exception("Boom!")
            with self.assertRaises(Exception):
                headers = {}
                data = utils.decode_payload(
                    payload,
                    compression=compression_mime_type,
                    content_type=content_type,
                    content_encoding=content_encoding,
                )

        with unittest.mock.patch("gestalt.amq.utils.loads") as mock_loads:
            mock_loads.side_effect = Exception("Boom!")
            with self.assertRaises(Exception):
                data = utils.decode_payload(
                    payload,
                    compression=compression_mime_type,
                    content_type=content_type,
                    content_encoding=content_encoding,
                )

    def test_text_payload_roundtrip(self):
        TEXT_DATA = "The Quick Brown Fox Jumps Over The Lazy Dog"
        codecs = compression.registry.compressors
        compression_names = list(codecs.keys())

        for compression_name in compression_names:
            with self.subTest(f"Check compressing payload using {compression_name}"):
                compression_mime_type = codecs[compression_name].content_type
                headers = {}
                payload, content_type, content_encoding = utils.encode_payload(
                    TEXT_DATA,
                    content_type=serialization.CONTENT_TYPE_TEXT,
                    compression=compression_name,
                    headers=headers,
                )
                if compression_name:
                    self.assertIn("compression", headers)

                data = utils.decode_payload(
                    payload,
                    compression=compression_mime_type,
                    content_type=content_type,
                    content_encoding=content_encoding,
                )
                self.assertEqual(data, TEXT_DATA)

    def test_json_payload_roundtrip(self):
        JSON_DATA = dict(latitude=130.0, longitude=-30.0, altitude=50.0)
        codecs = compression.registry.compressors
        compression_names = list(codecs.keys())

        for compression_name in compression_names:
            with self.subTest(f"Check compressing payload using {compression_name}"):
                compression_mime_type = codecs[compression_name].content_type
                headers = {}
                payload, content_type, content_encoding = utils.encode_payload(
                    JSON_DATA,
                    content_type=serialization.CONTENT_TYPE_JSON,
                    compression=compression_name,
                    headers=headers,
                )
                if compression_name:
                    self.assertIn("compression", headers)

                data = utils.decode_payload(
                    payload,
                    compression=compression_mime_type,
                    content_type=content_type,
                    content_encoding=content_encoding,
                )
                self.assertEqual(data, JSON_DATA)

    @unittest.skipUnless(serialization.have_msgpack, "requires msgpack")
    def test_msgpack_payload_roundtrip(self):
        MSGPACK_DATA = dict(latitude=130.0, longitude=-30.0, altitude=50.0)

        # Msgpack is already a compact binary format so there is nothing to
        # be gained by compressing it further.

        headers = {}
        payload, content_type, content_encoding = utils.encode_payload(
            MSGPACK_DATA,
            content_type=serialization.CONTENT_TYPE_MSGPACK,
            headers=headers,
        )
        self.assertNotIn("compression", headers)

        data = utils.decode_payload(
            payload, content_type=content_type, content_encoding=content_encoding
        )
        self.assertEqual(data, MSGPACK_DATA)

    @unittest.skipUnless(serialization.have_yaml, "requires yaml")
    def test_yaml_payload_roundtrip(self):
        YAML_DATA = dict(latitude=130.0, longitude=-30.0, altitude=50.0)
        codecs = compression.registry.compressors
        compression_names = list(codecs.keys())

        for compression_name in compression_names:
            with self.subTest(f"Check compressing payload using {compression_name}"):
                compression_mime_type = codecs[compression_name].content_type
                headers = {}
                payload, content_type, content_encoding = utils.encode_payload(
                    YAML_DATA,
                    content_type=serialization.CONTENT_TYPE_YAML,
                    compression=compression_name,
                    headers=headers,
                )
                if compression_name:
                    self.assertIn("compression", headers)

                data = utils.decode_payload(
                    payload,
                    compression=compression_mime_type,
                    content_type=content_type,
                    content_encoding=content_encoding,
                )
                self.assertEqual(data, YAML_DATA)

    @unittest.skipUnless(serialization.have_protobuf, "requires google protobuf")
    def test_protobuf_payload_roundtrip(self):
        from position_pb2 import Position

        PROTOBUF_DATA = Position(
            latitude=130.0, longitude=-30.0, altitude=50.0, status=Position.SIMULATED
        )

        serializer = serialization.registry.get_serializer(
            serialization.CONTENT_TYPE_PROTOBUF
        )
        type_identifier = serializer.registry.register_message(Position)

        # Protobuf is already a compact binary format so there is nothing
        # to be gained by compressing it further.
        headers = {}
        payload, content_type, content_encoding = utils.encode_payload(
            PROTOBUF_DATA,
            content_type=serialization.CONTENT_TYPE_PROTOBUF,
            headers=headers,
        )
        self.assertNotIn("compression", headers)
        self.assertIn("x-type-id", headers)

        data = utils.decode_payload(
            payload,
            content_type=content_type,
            content_encoding=content_encoding,
            type_identifier=headers["x-type-id"],
        )
        self.assertEqual(data, PROTOBUF_DATA)

    @unittest.skipUnless(serialization.have_avro, "requires avro")
    def test_avro_payload_roundtrip(self):

        # Add schema to serializer schema registry
        message_schema = {
            "namespace": "unittest.test_amq_utils",
            "type": "record",
            "name": "Position",
            "fields": [
                {"name": "latitude", "type": ["float", "null"]},
                {"name": "longitude", "type": ["float", "null"]},
                {"name": "altitude", "type": ["float", "null"]},
            ],
        }

        serializer = serialization.registry.get_serializer(
            serialization.CONTENT_TYPE_AVRO
        )
        type_identifier = serializer.registry.register_message(message_schema)

        AVRO_DATA = dict(latitude=130.0, longitude=-30.0, altitude=50.0)
        codecs = compression.registry.compressors
        compression_names = list(codecs.keys())

        # Avro is already a compact binary format so there may not be much
        # reason to compress it further, but whatever...
        for compression_name in compression_names:
            with self.subTest(f"Check compressing payload using {compression_name}"):
                compression_mime_type = codecs[compression_name].content_type

                # When content-type is AVRO a type identifier must be supplied.
                # Confirm an exception is raised if it is not provided.
                with self.assertRaises(Exception):
                    headers = {}
                    payload, content_type, content_encoding = utils.encode_payload(
                        AVRO_DATA,
                        content_type=serialization.CONTENT_TYPE_AVRO,
                        compression=compression_name,
                        headers=headers,
                    )

                headers = {}
                payload, content_type, content_encoding = utils.encode_payload(
                    AVRO_DATA,
                    content_type=serialization.CONTENT_TYPE_AVRO,
                    compression=compression_name,
                    headers=headers,
                    type_identifier=type_identifier,
                )
                if compression_name:
                    self.assertIn("compression", headers)

                self.assertIn("x-type-id", headers)

                data = utils.decode_payload(
                    payload,
                    compression=compression_mime_type,
                    content_type=content_type,
                    content_encoding=content_encoding,
                    type_identifier=headers["x-type-id"],
                )
                self.assertEqual(data, AVRO_DATA)

    def test_create_url_without_args(self):
        url = utils.build_amqp_url()
        self.assertEqual(url, "amqp://guest:guest@127.0.0.1:5672//")

    def test_create_url_with_connection_attempts(self):
        url = utils.build_amqp_url(connection_attempts=5)
        self.assertEqual(
            url, "amqp://guest:guest@127.0.0.1:5672//?connection_attempts=5"
        )

    def test_create_url_with_heartbeat_interval(self):
        url = utils.build_amqp_url(heartbeat_interval=2)
        self.assertEqual(
            url, "amqp://guest:guest@127.0.0.1:5672//?heartbeat_interval=2"
        )

    def test_create_url_with_connection_attempts_andheartbeat_interval(self):
        url = utils.build_amqp_url(connection_attempts=5, heartbeat_interval=2)
        self.assertEqual(
            url,
            "amqp://guest:guest@127.0.0.1:5672//?connection_attempts=5&heartbeat_interval=2",
        )

    def test_create_url_with_virtual_host(self):
        url = utils.build_amqp_url(virtual_host="vhost")
        self.assertEqual(url, "amqp://guest:guest@127.0.0.1:5672/vhost")

    def test_create_url_without_virtual_host(self):
        url = utils.build_amqp_url(
            user="myuser", password="mypass", host="my.host", port=5673
        )
        self.assertEqual(url, "amqp://myuser:mypass@my.host:5673//")

    def test_create_url_with_env_settings(self):
        values = {
            "RABBITMQ_USER": "user1",
            "RABBITMQ_PASS": "user1_password",
            "RABBITMQ_HOST": "rando.host",
            "RABBITMQ_PORT": "15672",
        }
        with unittest.mock.patch.dict("os.environ", values=values, clear=True):
            url = utils.build_amqp_url()
            self.assertEqual(url, "amqp://user1:user1_password@rando.host:15672//")
