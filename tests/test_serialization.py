import json
import unittest
from gestalt import serialization


class SerializationTestCase(unittest.TestCase):
    def test_text_serialization_roundtrip(self):
        text_data = "The Quick Brown Fox Jumps Over The Lazy Dog"
        content_type, content_encoding, payload = serialization.dumps(text_data, "text")
        self.assertIsInstance(payload, bytes)
        self.assertEqual(content_type, serialization.CONTENT_TYPE_TEXT)
        self.assertEqual(content_encoding, "utf-8")
        recovered_data = serialization.loads(
            payload, content_type=content_type, content_encoding=content_encoding
        )
        self.assertEqual(text_data, recovered_data)

    def test_json_serialization_roundtrip(self):
        json_data = {
            "string": "The quick brown fox jumps over the lazy dog",
            "int": 10,
            "float": 3.14159265,
            "unicode": "Thé quick brown fox jumps over thé lazy dog",
            "list": ["george", "jerry", "elaine", "cosmo"],
        }
        content_type, content_encoding, payload = serialization.dumps(json_data, "json")
        self.assertIsInstance(payload, bytes)
        self.assertEqual(content_type, serialization.CONTENT_TYPE_JSON)
        self.assertEqual(content_encoding, "utf-8")
        recovered_data = serialization.loads(
            payload, content_type=content_type, content_encoding=content_encoding
        )
        self.assertEqual(json_data, recovered_data)

    @unittest.skipUnless(serialization.have_msgpack, "requires msgpack")
    def test_msgpack_serialization_roundtrip(self):
        msgpack_data = {
            "string": "The quick brown fox jumps over the lazy dog",
            "int": 10,
            "float": 3.14159265,
            "unicode": "Thé quick brown fox jumps over thé lazy dog",
            "list": ["george", "jerry", "elaine", "cosmo"],
        }
        content_type, content_encoding, payload = serialization.dumps(
            msgpack_data, "msgpack"
        )
        self.assertIsInstance(payload, bytes)
        self.assertEqual(content_type, serialization.CONTENT_TYPE_MSGPACK)
        self.assertEqual(content_encoding, "binary")
        recovered_data = serialization.loads(
            payload, content_type=content_type, content_encoding=content_encoding
        )
        self.assertEqual(msgpack_data, recovered_data)

    @unittest.skipUnless(serialization.have_yaml, "requires yaml")
    def test_yaml_serialization_roundtrip(self):
        yaml_data = {
            "string": "The quick brown fox jumps over the lazy dog",
            "int": 10,
            "float": 3.14159265,
            "unicode": "Thé quick brown fox jumps over thé lazy dog",
            "list": ["george", "jerry", "elaine", "cosmo"],
        }
        content_type, content_encoding, payload = serialization.dumps(yaml_data, "yaml")
        self.assertIsInstance(payload, bytes)
        self.assertEqual(content_type, serialization.CONTENT_TYPE_YAML)
        self.assertEqual(content_encoding, "utf-8")
        recovered_data = serialization.loads(
            payload, content_type=content_type, content_encoding=content_encoding
        )
        self.assertEqual(yaml_data, recovered_data)

    @unittest.skipUnless(serialization.have_avro, "requires avro")
    def test_avro_serialization_roundtrip(self):

        # Add schema to serializer schema registry
        message_schema = {
            "namespace": "unittest.serialization",
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "string", "type": "string"},
                {"name": "int", "type": ["int", "null"]},
                {"name": "float", "type": ["float", "null"]},
                {"name": "unicode", "type": ["string", "null"]},
                {"name": "list", "type": {"type": "array", "items": "string"}},
            ],
        }

        serializer = serialization.registry.get_serializer("avro")
        type_identifier = serializer.registry.register_message(
            message_schema, type_identifier=1
        )

        avro_data = {
            "string": "The quick brown fox jumps over the lazy dog",
            "int": 10,
            "float": 3.14159265,
            "unicode": "Thé quick brown fox jumps over thé lazy dog",
            "list": ["george", "jerry", "elaine", "cosmo"],
        }

        content_type, content_encoding, payload = serialization.dumps(
            avro_data, "avro", type_identifier=type_identifier
        )
        self.assertIsInstance(payload, bytes)
        self.assertEqual(content_type, serialization.CONTENT_TYPE_AVRO)
        self.assertEqual(content_encoding, "binary")
        recovered_data = serialization.loads(
            payload,
            content_type=content_type,
            content_encoding=content_encoding,
            type_identifier=type_identifier,
        )
        self.assertEqual(avro_data["string"], recovered_data["string"])
        self.assertEqual(avro_data["int"], recovered_data["int"])
        self.assertAlmostEqual(avro_data["float"], recovered_data["float"], places=6)
        self.assertEqual(avro_data["unicode"], recovered_data["unicode"])
        self.assertEqual(avro_data["list"], recovered_data["list"])

    @unittest.skipUnless(serialization.have_protobuf, "requires google protobuf")
    def test_protobuf_serialization_roundtrip(self):
        from position_pb2 import Position

        protobuf_data = Position(
            latitude=130.0, longitude=-30.0, altitude=50.0, status=Position.SIMULATED
        )

        serializer = serialization.registry.get_serializer("protobuf")
        type_identifier = serializer.registry.register_message(
            Position, type_identifier=1
        )

        content_type, content_encoding, payload = serialization.dumps(
            protobuf_data, "protobuf", type_identifier=type_identifier
        )
        self.assertIsInstance(payload, bytes)
        self.assertEqual(content_type, serialization.CONTENT_TYPE_PROTOBUF)
        self.assertEqual(content_encoding, "binary")
        recovered_data = serialization.loads(
            payload,
            content_type=content_type,
            content_encoding=content_encoding,
            type_identifier=type_identifier,
        )
        self.assertEqual(protobuf_data, recovered_data)
