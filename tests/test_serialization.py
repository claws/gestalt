import json
import unittest
from gestalt import serialization


class SerializationTestCase(unittest.TestCase):
    def tearDown(self):
        serialization.registry.set_default("json")

    def test_expected_codecs_are_present(self):
        codecs = serialization.registry.serializers
        # Some codecs are always expected be present, confirm they are
        expected_codecs = (None, "text", "json")
        for codec_name in expected_codecs:
            with self.subTest(f"Check that {codec_name} is present"):
                self.assertIn(codec_name, codecs)

    def test_expected_codec_attributes(self):
        codecs = serialization.registry.serializers
        for name, settings in codecs.items():
            with self.subTest(f"Check that {name} has expected attributes"):
                for key in ("content_type", "content_encoding", "serializer"):
                    self.assertTrue(hasattr(settings, key))

    def test_fetch_serializer_by_name_or_type(self):
        codecs = serialization.registry.serializers

        for name in codecs.keys():
            with self.subTest(f"Check fetch using '{name}'"):
                compressor = serialization.registry.get_serializer(name)

        for content_type, _content_encoding, _serializer in codecs.values():
            with self.subTest(f"Check fetch using '{content_type}'"):
                compressor = serialization.registry.get_serializer(content_type)

    def test_fetch_codec_by_name_or_type(self):
        codecs = serialization.registry.serializers

        for name in codecs.keys():
            with self.subTest(f"Check fetch using '{name}'"):
                compressor = serialization.registry.get_codec(name)

        for content_type, _content_encoding, _serializer in codecs.values():
            with self.subTest(f"Check fetch using '{content_type}'"):
                compressor = serialization.registry.get_codec(content_type)

    def test_register_invalid_serializer(self):
        class InvalidSerializer(object):
            pass

        serializer = InvalidSerializer()
        with self.assertRaises(Exception) as cm:
            serialization.registry.register(
                "invalid",
                serializer,
                content_type="application/invalid",
                content_encoding="utf-8",
            )
        self.assertIn("Expected an instance of ISerializer", str(cm.exception))

    def test_fetch_codec_with_invalid_name_or_type(self):
        with self.assertRaises(Exception) as cm:
            serialization.registry.get_codec("invalid")
        self.assertIn("Invalid serializer", str(cm.exception))

    def test_fetch_serializer_with_invalid_name_or_type(self):
        with self.assertRaises(Exception) as cm:
            serialization.registry.get_serializer("invalid")
        self.assertIn("Invalid serializer", str(cm.exception))

    def test_loads_with_no_data(self):
        # No functionality is expected to be executed when no data needs
        # to be deserialized.
        serialization.loads(b"", "invalid", "unused-content-type")

    def test_loads_with_invalid_name_or_type(self):
        with self.assertRaises(Exception) as cm:
            serialization.loads(b"a", "invalid", "unused-content-type")
        self.assertIn("Invalid serializer", str(cm.exception))

    def test_dumps_with_invalid_name_or_type(self):
        with self.assertRaises(Exception) as cm:
            serialization.dumps(b"", "invalid")
        self.assertIn("Invalid serializer", str(cm.exception))

    def test_dumps_with_unspecified_name_or_type(self):
        content_type, content_encoding, payload = serialization.dumps(b"")
        self.assertEqual(content_type, serialization.CONTENT_TYPE_DATA)
        self.assertEqual(content_encoding, "binary")

        content_type, content_encoding, payload = serialization.dumps("")
        self.assertEqual(content_type, serialization.CONTENT_TYPE_TEXT)
        self.assertEqual(content_encoding, "utf-8")

        test_value = {"a": "a_string", "b": 42}
        content_type, content_encoding, payload = serialization.dumps(test_value)
        self.assertEqual(content_type, serialization.CONTENT_TYPE_JSON)
        self.assertEqual(content_encoding, "utf-8")

    def test_none_serialization_roundtrip(self):
        # Change default from JSON to None so we test the correct serializer
        serialization.registry.set_default(None)

        text_data = "The Quick Brown Fox Jumps Over The Lazy Dog"
        content_type, content_encoding, payload = serialization.dumps(text_data)
        self.assertIsInstance(payload, bytes)
        self.assertEqual(content_type, serialization.CONTENT_TYPE_TEXT)
        self.assertEqual(content_encoding, "utf-8")
        recovered_data = serialization.loads(
            payload, content_type=content_type, content_encoding=content_encoding
        )
        self.assertEqual(text_data, recovered_data)

        binary_data = text_data.encode()
        content_type, content_encoding, payload = serialization.dumps(binary_data)
        self.assertIsInstance(payload, bytes)
        self.assertEqual(content_type, serialization.CONTENT_TYPE_DATA)
        self.assertEqual(content_encoding, "binary")
        recovered_data = serialization.loads(
            payload, content_type=content_type, content_encoding=content_encoding
        )
        self.assertEqual(binary_data, recovered_data)

        # check exception is raised when bytes are not passed in
        with self.assertRaises(Exception) as cm:
            content_type, content_encoding, payload = serialization.dumps({})
        self.assertIn("Can only serialize bytes", str(cm.exception))

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
