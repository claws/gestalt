""" This module contains compression utilities. """

import abc
import zlib
from typing import Any, Callable, Dict, Optional, Tuple, Union

try:
    import bz2

    have_bz2 = True
except ImportError:
    have_bz2 = False

try:
    import lzma

    have_lzma = True
except ImportError:
    have_lzma = False

try:
    import brotli

    have_brotli = True
except ImportError:
    have_brotli = False

try:
    import snappy

    have_snappy = True
except ImportError:
    have_snappy = False


CompressorType = Callable[[bytes], bytes]
DecompressorType = Callable[[bytes], bytes]

COMPRESSION_NONE = "none"
COMPRESSION_GZIP = "applications/x-gzip"
COMPRESSION_BZ2 = "applications/x-bz2"
COMPRESSION_LZMA = "applications/x-lzma"
COMPRESSION_BROTLI = "applications/x-brotli"
COMPRESSION_SNAPPY = "application/x-snappy"
COMPRESSION_ZLIB = "application/zlib"
COMPRESSION_DEFLATE = "application/deflate"


class ICompressor(abc.ABC):
    """
    This class represents the base interface for a compressor.
    """

    @abc.abstractmethod  # pragma: no branch
    def compress(self, data):
        """ Returns compressed data as a bytes object. """

    @abc.abstractmethod  # pragma: no branch
    def decompress(self, data):
        """ Returns decompressed data """


class CompressorRegistry(object):
    """ This registry keeps track of compression strategies.

    A content-type string is mapped to an encoder and decoder.
    """

    def __init__(self) -> None:
        self._encoders = {}  # type: Dict[Optional[str], CompressorType]
        self._decoders = {}  # type: Dict[Optional[str], DecompressorType]
        self._supported_codecs = {}  # type: Dict[Optional[str], Dict]
        self._default_codec = None
        self.type_to_name = {}  # type: Dict[Optional[str], Optional[str]]
        self.name_to_type = {}  # type: Dict[Optional[str], Optional[str]]

    def register(
        self,
        name: Union[str, None],
        encoder: Callable,
        decoder: Callable,
        content_type: str,
    ) -> None:
        """ Register a new encoder/decoder.

        :param name (str): A convenience name for the compression method.
        :param encoder (callable): A method that will be used to compress
          data. If :const:`None` then encoding will not be possible.
        :param decoder (Callable): A method that will be used to decompress
          previously compressed data. If :const:`None` then decoding
          will not be possible.
        :param content_type (str): The mime-type describing the serialized
              structure.
        """
        if encoder:
            self._encoders[content_type] = encoder
        if decoder:
            self._decoders[content_type] = decoder

        self._supported_codecs[name] = {
            "encoder": encoder is not None,
            "decoder": decoder is not None,
            "content_type": content_type,
        }

        self.type_to_name[content_type] = name
        self.name_to_type[name] = content_type

    def _set_default_compressor(self, compression: Optional[str]) -> None:
        """ Set the default compression method used by this library.

        :param compression: The convenience name or the mime-type for the
          compression strategy.

        Raises:
            Exception: If the compression method requested is not available.
        """
        if compression in self.name_to_type:
            name = compression
            content_type = self.name_to_type[compression]
        elif compression in self.type_to_name:
            content_type = compression
            name = self.type_to_name[content_type]
        else:
            raise Exception(f"Invalid compressor '{compression}'' requested")

        if content_type not in self._encoders:
            raise Exception(f"No encoder installed for {content_type}")

        self._default_codec = compression

    def get_supported_codecs(self, both: bool = False):
        """ Return a dict of the available compression names containing
        values representing whether they have both an encoder and a decoder
        registered. E.g. {name: {'encoder': False, 'decoder':True}}

        :param both: When this optional argument is True only codecs that
          have an encoder and a decoder are returned.
        """
        if both:
            supported_codecs = {}
            for name, settings in self._supported_codecs.items():
                if settings["encoder"] and settings["decoder"]:
                    supported_codecs[name] = settings
        else:
            supported_codecs = self._supported_codecs
        return supported_codecs

    def get_compressor(self, compression: Optional[str]) -> Tuple[str, ICompressor]:
        """ Return a reference to a specific compressor

        :param compression: The convenience name or the mime-type for the
          compression strategy.

        :returns: The compression mime-type and a compressor function

        Raises:
            Exception: If the compression name or the mime-type requested is
            not available.
        """

        if compression in self.name_to_type:
            name = compression
            content_type = self.name_to_type[compression]
        elif compression in self.type_to_name:
            content_type = compression
            name = self.type_to_name[content_type]
        else:
            raise Exception(f"Invalid compressor '{compression}'' requested")

        try:
            return content_type, self._encoders[content_type]
        except KeyError:
            raise Exception(f"Invalid compressor '{content_type}' requested") from None

    def get_decompressor(self, compression: Optional[str]) -> Tuple[str, ICompressor]:
        """ Return a reference to a specific decompressor

        :param compression: The convenience name or the mime-type for the
          compression strategy.

        :returns: The compression mime-type and a decompressor function

        Raises:
            Exception: If the compression name or the mime-type requested is
            not available.
        """
        if compression in self.name_to_type:
            name = compression
            content_type = self.name_to_type[compression]
        elif compression in self.type_to_name:
            content_type = compression
            name = self.type_to_name[content_type]
        else:
            raise Exception(f"Invalid decompressor '{compression}'' requested")

        try:
            return content_type, self._decoders[content_type]
        except KeyError:
            raise Exception(
                f"Invalid decompressor '{content_type}' requested"
            ) from None

    def compress(
        self, data: Any, compression: Optional[str] = None
    ) -> Tuple[str, bytes]:
        """ Compress some data.

        Compress data into a bytes object suitable for sending as an AMQP message body.

        :param data: The message data to send.

        :param compression: The convenience name or the mime-type for the
          compression strategy. Defaults to none.

        :returns: A tuple containing a string specifying the compression mime-type
          (e.g. `application/gzip`) and a bytes object representing the compressed data.

        Raises:
            Exception: If the compression method requested is not available.
        """
        content_type, compress = self.get_compressor(compression)
        payload = compress(data)
        return content_type, payload

    def decompress(
        self, data: bytes, compression: Optional[str] = None, **kwargs
    ) -> Tuple[str, bytes]:
        """ Decompress some data.

        Decompress a data blob that was compressed using `compress` based on `compression`.

        :param data (bytes, buffer, str): The message data to decompress.

        :param compression: The convenience name or the mime-type for the
          compression strategy.

        Raises:
            Exception: If the decompression method requested is not available.

        Returns:
            Any: Decompressed data.
        """
        content_type, decompress = self.get_decompressor(compression)
        payload = decompress(data)
        return content_type, payload


def register_none(registry: CompressorRegistry):
    """ The compression you have when you don't want compression. """

    class NoneCompressor(ICompressor):
        def compress(self, data):
            """
            Return data as a bytes object.
            """
            if not isinstance(data, bytes):
                raise Exception(f"Can only compress bytes type, got {type(data)}")

            return data

        def decompress(self, data):
            return data

    compressor = NoneCompressor()
    registry.register(None, compressor.compress, compressor.decompress, None)


def register_zlib(registry: CompressorRegistry):
    """ Register a compressor/decompressor for zlib compression. """

    class ZlibCompressor(ICompressor):
        def compress(self, data):
            """ Create a RFC 1950 data format (zlib) compressor and compress
            some data.

            After calling flush the decompressor can't be used again. Hence,
            a new decompressor is created for each use.

            :return: data as a bytes object.
            """
            if not isinstance(data, bytes):
                raise Exception(
                    "Can only compress bytes type, got {}".format(type(data))
                )

            compressor = zlib.compressobj(level=9, wbits=zlib.MAX_WBITS)
            data = compressor.compress(data) + compressor.flush()
            return data

        def decompress(self, data):
            """ Create a RFC 1950 data format (zlib) decompressor and
            decompress some data.

            After calling flush the decompressor can't be used again. Hence,
            a new decompressor is created for each use.

            :return: data as a bytes object.
            """
            decompressor = zlib.decompressobj(zlib.MAX_WBITS)
            data = decompressor.decompress(data) + decompressor.flush()
            return data

    compressor = ZlibCompressor()
    registry.register(
        "zlib", compressor.compress, compressor.decompress, COMPRESSION_ZLIB
    )


def register_deflate(registry: CompressorRegistry):
    """ Register a compressor/decompressor for deflate compression. """

    class DeflateCompressor(ICompressor):
        def compress(self, data):
            """ Create a RFC 1951 data format (deflate) compressor and compress
            some data.

            After calling flush the decompressor can't be used again. Hence,
            a new decompressor is created for each use.

            :return: data as a bytes object.
            """
            if not isinstance(data, bytes):
                raise Exception(
                    "Can only compress bytes type, got {}".format(type(data))
                )

            compressor = zlib.compressobj(level=9, wbits=-zlib.MAX_WBITS)
            data = compressor.compress(data) + compressor.flush()
            return data

        def decompress(self, data):
            """ Create a RFC 1951 data format (deflate) decompressor and
            decompress some data.

            After calling flush the decompressor can't be used again. Hence,
            a new decompressor is created for each use.

            :return: data as a bytes object.
            """
            decompressor = zlib.decompressobj(-zlib.MAX_WBITS)
            data = decompressor.decompress(data) + decompressor.flush()
            return data

    compressor = DeflateCompressor()
    registry.register(
        "deflate", compressor.compress, compressor.decompress, COMPRESSION_DEFLATE
    )


def register_gzip(registry: CompressorRegistry):
    """ Register a compressor/decompressor for gzip compression. """

    class GzipCompressor(ICompressor):
        def compress(self, data):
            """ Create a RFC 1952 data format (gzip) compressor and compress
            some data.

            After calling flush the compressor can't be used again. Hence,
            a new compressor is created for each use.

            :return: data as a bytes object.
            """
            if not isinstance(data, bytes):
                raise Exception(f"Can only compress bytes type, got {type(data)}")

            compressor = zlib.compressobj(level=9, wbits=zlib.MAX_WBITS | 16)
            data = compressor.compress(data) + compressor.flush()
            return data

        def decompress(self, data):
            """ Create a RFC 1952 data format (gzip) decompressor and
            decompress some data.

            After calling flush the decompressor can't be used again. Hence,
            a new decompressor is created for each use.

            :return: data as a bytes object.
            """
            decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)
            data = decompressor.decompress(data) + decompressor.flush()
            return data

    compressor = GzipCompressor()
    registry.register(
        "gzip", compressor.compress, compressor.decompress, COMPRESSION_GZIP
    )


def register_bz2(registry: CompressorRegistry):
    """ Register a compressor/decompressor for bz2 compression. """

    if have_bz2:

        class Bz2Compressor(ICompressor):
            def compress(self, data):
                """ Create a bz2 compressor and compress some data.

                After calling flush the compressor can't be used again. Hence,
                a new compressor is created for each use.

                :return: data as a bytes object.
                """
                if not isinstance(data, bytes):
                    raise Exception(f"Can only compress bytes type, got {type(data)}")

                compressor = bz2.BZ2Compressor()
                data = compressor.compress(data) + compressor.flush()
                return data

            def decompress(self, data):
                """ Create a bz2 decompressor and decompress some data.

                :return: data as a bytes object.
                """
                decompressor = bz2.BZ2Decompressor()
                data = decompressor.decompress(data)
                return data

        compressor = Bz2Compressor()
        registry.register(
            "bzip2", compressor.compress, compressor.decompress, COMPRESSION_BZ2
        )


def register_lzma(registry: CompressorRegistry):
    """ Register a compressor/decompressor for lzma compression. """

    if have_lzma:

        class LzmaCompressor(ICompressor):
            def compress(self, data):
                """ Create a lzma compressor and compress some data.

                After calling flush the compressor can't be used again. Hence,
                a new compressor is created for each use.

                :return: data as a bytes object.
                """
                if not isinstance(data, bytes):
                    raise Exception(f"Can only compress bytes type, got {type(data)}")

                compressor = lzma.LZMACompressor()
                data = compressor.compress(data) + compressor.flush()
                return data

            def decompress(self, data):
                """ Create a lzma decompressor and decompress some data.

                :return: data as a bytes object.
                """
                decompressor = lzma.LZMADecompressor()
                data = decompressor.decompress(data)
                return data

        compressor = LzmaCompressor()
        registry.register(
            "lzma", compressor.compress, compressor.decompress, COMPRESSION_LZMA
        )


def register_brotli(registry: CompressorRegistry):
    """ Register a compressor/decompressor for brotli compression. """

    if have_brotli:

        class BrotliCompressor(ICompressor):
            def compress(self, data):
                """ Compress data using a brotli compressor.

                :return: data as a bytes object.
                """
                if not isinstance(data, bytes):
                    raise Exception(f"Can only compress bytes type, got {type(data)}")

                return brotli.compress(data)

            def decompress(self, data):
                """ Decompress data using a brotli decompressor.

                :return: data as a bytes object.
                """
                return brotli.decompress(data)

        compressor = BrotliCompressor()
        registry.register(
            "brotli", compressor.compress, compressor.decompress, COMPRESSION_BROTLI
        )


def register_snappy(registry: CompressorRegistry):
    """ Register a compressor/decompressor for snappy compression. """

    if have_snappy:

        class SnappyCompressor(ICompressor):
            def compress(self, data):
                """ Compress data using a snappy compressor.

                :return: data as a bytes object.
                """
                if not isinstance(data, bytes):
                    raise Exception(f"Can only compress bytes type, got {type(data)}")

                return snappy.compress(data)

            def decompress(self, data):
                """ Decompress data using a snappy decompressor.

                :return: data as a bytes object.
                """
                return snappy.uncompress(data)

        compressor = SnappyCompressor()
        registry.register(
            "snappy", compressor.compress, compressor.decompress, COMPRESSION_SNAPPY
        )


def initialize(registry: CompressorRegistry):
    """ Register compression methods and set a default """
    register_none(registry)
    register_zlib(registry)
    register_deflate(registry)
    register_gzip(registry)
    register_bz2(registry)
    register_lzma(registry)
    register_brotli(registry)
    register_snappy(registry)

    registry._set_default_compressor(None)


registry = CompressorRegistry()

compress = registry.compress

decompress = registry.decompress

initialize(registry)
