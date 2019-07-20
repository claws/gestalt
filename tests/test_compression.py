import unittest
from gestalt import compression


TEST_DATA = b"The Quick Brown Fox Jumps Over The Lazy Dog"


class CompressionTestCase(unittest.TestCase):
    def test_expected_codecs_are_present(self):
        codecs = compression.registry.get_supported_codecs()
        # Some codecs should always be present, confirm they are
        expected_codecs = (
            (None, None),
            ("zlib", compression.COMPRESSION_ZLIB),
            ("deflate", compression.COMPRESSION_DEFLATE),
            ("gzip", compression.COMPRESSION_GZIP),
            ("bzip2", compression.COMPRESSION_BZ2),
        )
        for expected_codec_name, expected_codec_content_type in expected_codecs:
            with self.subTest(f"Check that {expected_codec_name} is present"):
                self.assertIn(expected_codec_name, codecs)

    def test_expected_codec_attributes(self):
        codecs = compression.registry.get_supported_codecs()
        for name, settings in codecs.items():
            with self.subTest(f"Check that {name} has expected attributes"):
                for key in ("content_type", "encoder", "decoder"):
                    self.assertIn(key, settings)
                self.assertEqual(
                    compression.registry.name_to_type[name], settings["content_type"]
                )

    def test_compression_roundtrip(self):
        codecs = compression.registry.get_supported_codecs()
        for name, settings in codecs.items():
            with self.subTest(f"Check {name} compression roundtrip"):
                convenience_name = name
                mime_type = compression.registry.name_to_type[name]

                # The convenience name or the content_type can be used when
                # specifying compression method. Check both.
                for c_label in (convenience_name, mime_type):
                    content_type, payload = compression.compress(TEST_DATA, c_label)
                    self.assertEqual(content_type, mime_type)
                    if c_label is None:
                        self.assertEqual(TEST_DATA, payload)
                    else:
                        self.assertNotEqual(TEST_DATA, payload)
                    content_type, d = compression.decompress(payload, content_type)
                    self.assertEqual(content_type, mime_type)
                    self.assertEqual(d, TEST_DATA)

    # def test_none_compression(self):
    #     codecs = compression.registry.get_supported_codecs()
    #     self.assertIn(None, codecs)

    #     content_type, payload = compression.compress(TEST_DATA)
    #     self.assertIsNone(content_type)
    #     self.assertEqual(TEST_DATA, payload)
    #     content_type, d = compression.decompress(payload)
    #     self.assertIsNone(content_type)
    #     self.assertEqual(d, TEST_DATA)

    # def test_zlib_compression(self):
    #     convenience_name = "zlib"
    #     mime_type = compression.COMPRESSION_ZLIB

    #     for c_name in (convenience_name, mime_type):
    #         content_type, payload = compression.compress(TEST_DATA, c_name)
    #         self.assertEqual(content_type, mime_type)
    #         self.assertNotEqual(TEST_DATA, payload)
    #         content_type, d = compression.decompress(payload, content_type)
    #         self.assertEqual(content_type, mime_type)
    #         self.assertEqual(d, TEST_DATA)

    # def test_deflate_compression(self):
    #     convenience_name = "deflate"
    #     mime_type = compression.COMPRESSION_DEFLATE

    #     for c_name in (convenience_name, mime_type):
    #         content_type, payload = compression.compress(TEST_DATA, c_name)
    #         self.assertEqual(content_type, mime_type)
    #         self.assertNotEqual(TEST_DATA, payload)
    #         content_type, d = compression.decompress(payload, content_type)
    #         self.assertEqual(content_type, mime_type)
    #         self.assertEqual(d, TEST_DATA)

    # def test_gzip_compression(self):
    #     convenience_name = "gzip"
    #     mime_type = compression.COMPRESSION_GZIP

    #     for c_name in (convenience_name, mime_type):
    #         content_type, payload = compression.compress(TEST_DATA, c_name)
    #         self.assertEqual(content_type, mime_type)
    #         self.assertNotEqual(TEST_DATA, payload)
    #         content_type, d = compression.decompress(payload, content_type)
    #         self.assertEqual(content_type, mime_type)
    #         self.assertEqual(d, TEST_DATA)

    # @unittest.skipUnless(compression.have_bz2, "requires bz2")
    # def test_bzip2_compression(self):
    #     convenience_name = "bzip2"
    #     mime_type = compression.COMPRESSION_BZ2

    #     for c_name in (convenience_name, mime_type):
    #         content_type, payload = compression.compress(TEST_DATA, c_name)
    #         self.assertEqual(content_type, mime_type)
    #         self.assertNotEqual(TEST_DATA, payload)
    #         content_type, d = compression.decompress(payload, content_type)
    #         self.assertEqual(content_type, mime_type)
    #         self.assertEqual(d, TEST_DATA)

    @unittest.skipUnless(compression.have_lzma, "requires lzma")
    def test_lzma_compression(self):
        convenience_name = "lzma"
        mime_type = compression.COMPRESSION_LZMA

        for c_name in (convenience_name, mime_type):
            content_type, payload = compression.compress(TEST_DATA, c_name)
            self.assertEqual(content_type, mime_type)
            self.assertNotEqual(TEST_DATA, payload)
            content_type, d = compression.decompress(payload, content_type)
            self.assertEqual(content_type, mime_type)
            self.assertEqual(d, TEST_DATA)

    @unittest.skipUnless(compression.have_brotli, "requires brotli")
    def test_brotli_compression(self):
        convenience_name = "brotli"
        mime_type = compression.COMPRESSION_BROTLI

        for c_name in (convenience_name, mime_type):
            content_type, payload = compression.compress(TEST_DATA, c_name)
            self.assertEqual(content_type, mime_type)
            self.assertNotEqual(TEST_DATA, payload)
            content_type, d = compression.decompress(payload, content_type)
            self.assertEqual(content_type, mime_type)
            self.assertEqual(d, TEST_DATA)

    @unittest.skipUnless(compression.have_snappy, "requires snappy")
    def test_snappy_compression(self):
        convenience_name = "snappy"
        mime_type = compression.COMPRESSION_SNAPPY

        for c_name in (convenience_name, mime_type):
            content_type, payload = compression.compress(TEST_DATA, c_name)
            self.assertEqual(content_type, mime_type)
            self.assertNotEqual(TEST_DATA, payload)
            content_type, d = compression.decompress(payload, content_type)
            self.assertEqual(content_type, mime_type)
            self.assertEqual(d, TEST_DATA)
