""" This module contains compression utilities. """

import abc
import zlib
from collections import namedtuple
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

CodecType = Callable[[bytes], bytes]

COMPRESSION_NONE = "none"
COMPRESSION_GZIP = "applications/x-gzip"
COMPRESSION_BZ2 = "applications/x-bz2"
COMPRESSION_LZMA = "applications/x-lzma"
COMPRESSION_BROTLI = "applications/x-brotli"
COMPRESSION_SNAPPY = "application/x-snappy"
COMPRESSION_ZLIB = "application/zlib"
COMPRESSION_DEFLATE = "application/deflate"


codec = namedtuple("codec", ("content_type", "compressor"))


class ICompressor(abc.ABC):
    """
    This class represents the base interface for a compressor.
    """

    @abc.abstractmethod  # pragma: no branch
    def compress(self, data):
        """ Returns compressed data """

    @abc.abstractmethod  # pragma: no branch
    def decompress(self, data):
        """ Returns decompressed data """


class CompressorRegistry(object):
    """ This registry keeps track of compression strategies.

    A convenience name or the specific content-type string can be used to
    reference a specific compressor that is capable of encoding and decoding.
    """

    def __init__(self) -> None:
        self._compressors = {}  # type: Dict[Optional[str], codec]
        self._default_codec = None  # type: Optional[str]
        self.type_to_name = {}  # type: Dict[Optional[str], Optional[str]]
        self.name_to_type = {}  # type: Dict[Optional[str], Optional[str]]

    def register(
        self,
        name: Union[str, None],
        compressor: ICompressor,
        content_type: Optional[str],
    ) -> None:
        """ Register a new encoder/decoder.

        :param name (str): A convenience name for the compression method.

        :param compressor: An object that implements the ICompressor interface
          that can compress and decompress data back into the original object.

        :param content_type (str): The mime-type describing the serialized
              structure.
        """
        if not isinstance(compressor, ICompressor):
            raise Exception(
                f"Invalid compressor '{name}'. Expected an instance of ICompressor"
            )

        self._compressors[name] = codec(content_type, compressor)

        # map convenience name to mime-type and back again.
        self.type_to_name[content_type] = name
        self.name_to_type[name] = content_type

    def set_default(self, name_or_type: Optional[str]) -> None:
        """ Set the default compression method used by this library.

        :param name_or_type: The convenience name or the mime-type for the
          compression strategy.

        Raises:
            Exception: If the name_or_type requested is not available.
        """
        name, content_type = self._resolve(name_or_type)
        self._default_codec = name

    @property
    def compressors(self):
        """ Return a dict of the available compressors (codecs) """
        return self._compressors

    def get_compressor(self, name_or_type: str):
        """ Return a specific compressor.

        :param name_or_type: The convenience name or the mime-type for the
          compression strategy. The value may be the alias name (e.g. zlib)
          or the mime-type (e.g. application/zlib).

        :returns: A compressor object that can encode and decode bytes
          using the named strategy.
        """
        name, content_type = self._resolve(name_or_type)
        return self._compressors[name].compressor

    def get_codec(self, name_or_type: str):
        """ Return codec attributes for a specific compressor.

        :param name_or_type: The convenience name or the mime-type for the
          compression strategy. The value may be the alias name (e.g. zlib)
          or the mime-type (e.g. application/zlib).

        :returns: A codec named tuple.
        """
        name, content_type = self._resolve(name_or_type)
        return self._compressors[name]

    def compress(
        self, data: Any, name_or_type: Optional[str] = None
    ) -> Tuple[Optional[str], bytes]:
        """ Compress some data.

        Compress data into a bytes object suitable for sending as a message body.

        :param data: The message data to send.

        :param name_or_type: The convenience name or the mime-type for the
          compression strategy. The value may be the alias name (e.g. zlib)
          or the mime-type (e.g. application/zlib). Defaults to none.

        :returns: A tuple containing a string specifying the compression mime-type
          (e.g. `application/gzip`) and a bytes object representing the compressed data.

        Raises:
            Exception: If the compression method requested is not available.
        """
        name, content_type = self._resolve(name_or_type)
        payload = self._compressors[name].compressor.compress(data)
        return content_type, payload

    def decompress(
        self, data: bytes, name_or_type: Optional[str] = None, **kwargs
    ) -> Tuple[Optional[str], bytes]:
        """ Decompress some data.

        Decompress a data blob that was compressed using `compress` based on
        `compression`.

        :param data (bytes, buffer, str): The message data to decompress.

        :param name_or_type: The convenience name or the mime-type for the
          compression strategy. The value may be the alias name (e.g. zlib)
          or the mime-type (e.g. application/zlib). Defaults to none.

        Raises:
            Exception: If the decompression method requested is not available.

        Returns:
            Any: Decompressed data.
        """
        name, content_type = self._resolve(name_or_type)
        payload = self._compressors[name].compressor.decompress(data)
        return content_type, payload

    def _resolve(
        self, name_or_type: Optional[str]
    ) -> Tuple[Optional[str], Optional[str]]:
        """ Resolve the compression name and mime-type.

        :param name_or_type: The convenience name or the mime-type for the
          compression strategy. The value may be the alias name (e.g. zlib)
          or the mime-type (e.g. application/zlib).

        Raises:
            Exception: If the compression method requested is not available.
        """
        if name_or_type in self.name_to_type:
            name = name_or_type
            content_type = self.name_to_type[name_or_type]
        elif name_or_type in self.type_to_name:
            content_type = name_or_type
            name = self.type_to_name[content_type]
        else:
            raise Exception(f"Invalid compressor '{name_or_type}'")

        return name, content_type


def register_none(registry: CompressorRegistry):
    """ The compression you have when you don't want compression. """

    class NoneCompressor(ICompressor):
        def compress(self, data):
            """
            Return data as a bytes object.
            """
            if not isinstance(data, bytes):
                raise Exception(f"Can only compress bytes, got {type(data)}")

            return data

        def decompress(self, data):
            return data

    compressor = NoneCompressor()
    registry.register(None, compressor, None)


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
                raise Exception("Can only compress bytes, got {}".format(type(data)))

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
    registry.register("zlib", compressor, COMPRESSION_ZLIB)


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
                raise Exception("Can only compress bytes, got {}".format(type(data)))

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
    registry.register("deflate", compressor, COMPRESSION_DEFLATE)


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
                raise Exception(f"Can only compress bytes, got {type(data)}")

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
    registry.register("gzip", compressor, COMPRESSION_GZIP)


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
                    raise Exception(f"Can only compress bytes, got {type(data)}")

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
        registry.register("bzip2", compressor, COMPRESSION_BZ2)


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
                    raise Exception(f"Can only compress bytes, got {type(data)}")

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
        registry.register("lzma", compressor, COMPRESSION_LZMA)


def register_brotli(registry: CompressorRegistry):
    """ Register a compressor/decompressor for brotli compression. """

    if have_brotli:

        class BrotliCompressor(ICompressor):
            def compress(self, data):
                """ Compress data using a brotli compressor.

                :return: data as a bytes object.
                """
                if not isinstance(data, bytes):
                    raise Exception(f"Can only compress bytes, got {type(data)}")

                return brotli.compress(data)

            def decompress(self, data):
                """ Decompress data using a brotli decompressor.

                :return: data as a bytes object.
                """
                return brotli.decompress(data)

        compressor = BrotliCompressor()
        registry.register("brotli", compressor, COMPRESSION_BROTLI)


def register_snappy(registry: CompressorRegistry):
    """ Register a compressor/decompressor for snappy compression. """

    if have_snappy:

        class SnappyCompressor(ICompressor):
            def compress(self, data):
                """ Compress data using a snappy compressor.

                :return: data as a bytes object.
                """
                if not isinstance(data, bytes):
                    raise Exception(f"Can only compress bytes, got {type(data)}")

                return snappy.compress(data)

            def decompress(self, data):
                """ Decompress data using a snappy decompressor.

                :return: data as a bytes object.
                """
                return snappy.uncompress(data)

        compressor = SnappyCompressor()
        registry.register("snappy", compressor, COMPRESSION_SNAPPY)


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

    registry.set_default(None)


registry = CompressorRegistry()

compress = registry.compress

decompress = registry.decompress

initialize(registry)
