import unittest
from gestalt import compression


TEST_DATA = b"The Quick Brown Fox Jumps Over The Lazy Dog"


class CompressionTestCase(unittest.TestCase):
    def test_expected_codecs_are_present(self):
        codecs = compression.registry.compressors
        # Some codecs are always expected be present, confirm they are
        expected_codecs = (None, "zlib", "deflate", "gzip", "bzip2")
        for codec_name in expected_codecs:
            with self.subTest(f"Check that {codec_name} is present"):
                self.assertIn(codec_name, codecs)

    def test_expected_codec_attributes(self):
        codecs = compression.registry.compressors
        for name, settings in codecs.items():
            with self.subTest(f"Check that {name} has expected attributes"):
                for key in ("content_type", "compressor"):
                    self.assertTrue(hasattr(settings, key))

    def test_fetch_compressor_by_name_or_type(self):
        codecs = compression.registry.compressors

        for name in codecs.keys():
            with self.subTest(f"Check fetch using '{name}'"):
                compressor = compression.registry.get_compressor(name)

        for content_type, _compressor in codecs.values():
            with self.subTest(f"Check fetch using '{content_type}'"):
                compressor = compression.registry.get_compressor(content_type)

    def test_fetch_codec_by_name_or_type(self):
        codecs = compression.registry.compressors

        for name in codecs.keys():
            with self.subTest(f"Check fetch using '{name}'"):
                compressor = compression.registry.get_codec(name)

        for content_type, _compressor in codecs.values():
            with self.subTest(f"Check fetch using '{content_type}'"):
                compressor = compression.registry.get_codec(content_type)

    def test_register_invalid_compressor(self):
        class InvalidCompressor(object):
            pass

        compressor = InvalidCompressor()
        with self.assertRaises(Exception) as cm:
            compression.registry.register(
                "invalid", compressor, content_type="application/invalid"
            )
        self.assertIn("Expected an instance of ICompressor", str(cm.exception))

    def test_fetch_codec_with_invalid_name_or_type(self):
        with self.assertRaises(Exception) as cm:
            compression.registry.get_codec("invalid")
        self.assertIn("Invalid compressor", str(cm.exception))

    def test_fetch_compressor_with_invalid_name_or_type(self):
        with self.assertRaises(Exception) as cm:
            compression.registry.get_compressor("invalid")
        self.assertIn("Invalid compressor", str(cm.exception))

    def test_decompress_with_invalid_name_or_type(self):
        with self.assertRaises(Exception) as cm:
            compression.decompress(b"a", "invalid")
        self.assertIn("Invalid compressor", str(cm.exception))

    def test_compress_with_invalid_name_or_type(self):
        with self.assertRaises(Exception) as cm:
            compression.compress(b"", "invalid")
        self.assertIn("Invalid compressor", str(cm.exception))

    def test_compress_with_unspecified_name_or_type(self):
        content_type, payload = compression.compress(b"")
        self.assertEqual(content_type, None)

    def test_compression_roundtrip(self):
        codecs = compression.registry.compressors
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

    def test_none_compression(self):
        codecs = compression.registry.compressors
        self.assertIn(None, codecs)

        # check exception is raised when bytes are not passed in
        with self.assertRaises(Exception) as cm:
            compression.compress({})
        self.assertIn("Can only compress bytes", str(cm.exception))

        # perform roundtrip check
        content_type, payload = compression.compress(TEST_DATA)
        self.assertIsNone(content_type)
        self.assertEqual(TEST_DATA, payload)
        content_type, d = compression.decompress(payload)
        self.assertIsNone(content_type)
        self.assertEqual(d, TEST_DATA)

    def test_zlib_compression(self):
        convenience_name = "zlib"
        mime_type = compression.COMPRESSION_ZLIB

        # check exception is raised when bytes are not passed in
        with self.assertRaises(Exception) as cm:
            compression.compress({}, mime_type)
        self.assertIn("Can only compress bytes", str(cm.exception))

        # perform roundtrip check
        for c_name in (convenience_name, mime_type):
            with self.subTest(f"Check zlib compression roundtrip using {c_name}"):
                content_type, payload = compression.compress(TEST_DATA, c_name)
                self.assertEqual(content_type, mime_type)
                self.assertNotEqual(TEST_DATA, payload)
                content_type, d = compression.decompress(payload, content_type)
                self.assertEqual(content_type, mime_type)
                self.assertEqual(d, TEST_DATA)

    def test_deflate_compression(self):
        convenience_name = "deflate"
        mime_type = compression.COMPRESSION_DEFLATE

        # check exception is raised when bytes are not passed in
        with self.assertRaises(Exception) as cm:
            compression.compress({}, mime_type)
        self.assertIn("Can only compress bytes", str(cm.exception))

        # perform roundtrip check
        for c_name in (convenience_name, mime_type):
            with self.subTest(f"Check deflate compression roundtrip using {c_name}"):
                content_type, payload = compression.compress(TEST_DATA, c_name)
                self.assertEqual(content_type, mime_type)
                self.assertNotEqual(TEST_DATA, payload)
                content_type, d = compression.decompress(payload, content_type)
                self.assertEqual(content_type, mime_type)
                self.assertEqual(d, TEST_DATA)

    def test_gzip_compression(self):
        convenience_name = "gzip"
        mime_type = compression.COMPRESSION_GZIP

        # check exception is raised when bytes are not passed in
        with self.assertRaises(Exception) as cm:
            compression.compress({}, mime_type)
        self.assertIn("Can only compress bytes", str(cm.exception))

        # perform roundtrip check
        for c_name in (convenience_name, mime_type):
            with self.subTest(f"Check gzip compression roundtrip using {c_name}"):
                content_type, payload = compression.compress(TEST_DATA, c_name)
                self.assertEqual(content_type, mime_type)
                self.assertNotEqual(TEST_DATA, payload)
                content_type, d = compression.decompress(payload, content_type)
                self.assertEqual(content_type, mime_type)
                self.assertEqual(d, TEST_DATA)

    @unittest.skipUnless(compression.have_bz2, "requires bz2")
    def test_bzip2_compression(self):
        convenience_name = "bzip2"
        mime_type = compression.COMPRESSION_BZ2

        # check exception is raised when bytes are not passed in
        with self.assertRaises(Exception) as cm:
            compression.compress({}, mime_type)
        self.assertIn("Can only compress bytes", str(cm.exception))

        # perform roundtrip check
        for c_name in (convenience_name, mime_type):
            with self.subTest(f"Check bzip2 compression roundtrip using {c_name}"):
                content_type, payload = compression.compress(TEST_DATA, c_name)
                self.assertEqual(content_type, mime_type)
                self.assertNotEqual(TEST_DATA, payload)
                content_type, d = compression.decompress(payload, content_type)
                self.assertEqual(content_type, mime_type)
                self.assertEqual(d, TEST_DATA)

    @unittest.skipUnless(compression.have_lzma, "requires lzma")
    def test_lzma_compression(self):
        convenience_name = "lzma"
        mime_type = compression.COMPRESSION_LZMA

        # check exception is raised when bytes are not passed in
        with self.assertRaises(Exception) as cm:
            compression.compress({}, mime_type)
        self.assertIn("Can only compress bytes", str(cm.exception))

        # perform roundtrip check
        for c_name in (convenience_name, mime_type):
            with self.subTest(f"Check lzma compression roundtrip using {c_name}"):
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

        # check exception is raised when bytes are not passed in
        with self.assertRaises(Exception) as cm:
            compression.compress({}, mime_type)
        self.assertIn("Can only compress bytes", str(cm.exception))

        # perform roundtrip check
        for c_name in (convenience_name, mime_type):
            with self.subTest(f"Check brotli compression roundtrip using {c_name}"):
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

        # check exception is raised when bytes are not passed in
        with self.assertRaises(Exception) as cm:
            compression.compress({}, mime_type)
        self.assertIn("Can only compress bytes", str(cm.exception))

        # perform roundtrip check
        for c_name in (convenience_name, mime_type):
            with self.subTest(f"Check snappy compression roundtrip using {c_name}"):
                content_type, payload = compression.compress(TEST_DATA, c_name)
                self.assertEqual(content_type, mime_type)
                self.assertNotEqual(TEST_DATA, payload)
                content_type, d = compression.decompress(payload, content_type)
                self.assertEqual(content_type, mime_type)
                self.assertEqual(d, TEST_DATA)
