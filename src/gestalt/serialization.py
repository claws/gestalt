import abc
import codecs
import io
import itertools
import json

from collections import namedtuple
from typing import Any, Callable, Optional

try:
    import avro

    have_avro = True
except ImportError:
    have_avro = False

try:
    import msgpack

    have_msgpack = True
except ImportError:
    have_msgpack = False

try:
    import yaml

    have_yaml = True
except ImportError:
    have_yaml = False

try:
    import google

    have_protobuf = True
except ImportError:
    have_protobuf = False


CONTENT_TYPE_AVRO = "application/x-avro"
CONTENT_TYPE_DATA = "application/data"
CONTENT_TYPE_JSON = "application/json"
CONTENT_TYPE_MSGPACK = "application/msgpack"
CONTENT_TYPE_PROTOBUF = "application/vnd.google.protobuf"
CONTENT_TYPE_TEXT = "text/plain"
CONTENT_TYPE_YAML = "application/yaml"


codec = namedtuple("codec", ("content_type", "content_encoding", "serializer"))


class ISerializer(abc.ABC):
    """
    This class represents the base interface for a serializer.
    """

    @abc.abstractmethod  # pragma: no branch
    def encode(self, data, **kwargs):
        """ Returns serialized data as a bytes object. """

    @abc.abstractmethod  # pragma: no branch
    def decode(self, data, **kwargs):
        """ Returns deserialized data """


class SerializerRegistry(object):
    """ This registry keeps track of serialization strategies.

    A content-type string is mapped to an encoder and decoder.
    """

    def __init__(self):
        self._serializers = {}
        self._default_codec = None
        self.type_to_name = {}
        self.name_to_type = {}

    def register(
        self,
        name: Optional[str],
        serializer: ISerializer,
        content_type: str,
        content_encoding: str = "utf-8",
    ):
        """ Register a new serializer.

        :param name: A convenience name for the serialization method.

        :param serializer: An object that implements the ISerializer interface
          that can encode objects and decode data back into the original object.

        :param content_type: The mime-type describing the serialized structure.

        :param content_encoding: The content encoding (character set) that
          the decoder method will be returning. Will usually be `utf-8` or `binary`.
        """
        if not isinstance(serializer, ISerializer):
            raise Exception(
                f"Invalid serializer '{name}'. Expected an instance of ISerializer"
            )

        self._serializers[name] = codec(content_type, content_encoding, serializer)

        # map convenience name to mime-type and back again.
        self.type_to_name[content_type] = name
        self.name_to_type[name] = content_type

    def _set_default_serializer(self, name: str):
        """ Set the default serialization method used by this library.

        :param name: The name of a registered serialization method (e.g. json)

        Raises:
            Exception: If the serialization method requested is not available.
        """
        if name not in self._serializers:
            raise Exception(f"Invalid serializer {name}")

        self._default_codec = name

    def get_supported_serializers(self):
        """ Return a dict of the available serializers (codecs) """
        return self._serializers

    def get_codec(self, name):
        try:
            return self._serializers[name]
        except KeyError:
            raise Exception("Serializer '{name}' does not exist") from None

    def get_serializer(self, name: str):
        try:
            return self._serializers[name].serializer
        except KeyError:
            raise Exception("Serializer '{name}' does not exist") from None

    def dumps(self, data: Any, serialization: str = None, **kwargs):
        """ Encode data.

        Serialize a data structure into a bytes object suitable for sending
        as an AMQP message body.

        :param data: The message data to send.

        :param serialization: A string representing the serialization strategy
          to apply to the data (e.g. ``json``, etc). If not specified then a
          best effort guess will be made. If data is a string then text will
          be used, if the data is bytes then data will be used, otherwise the
          default serializer will be used (JSON).

        Keywords:

          :param type_identifier: An integer that uniquely identifies a
            registered message.

        :returns: A string specifying the content type (e.g.,
          `application/json`), a string specifying the content encoding, (e.g.
          `utf-8`) and a three-item tuple containing the serialized data as
          bytes.

        Raises:
            Exception: If the serialization method requested is not available.
        """
        if serialization:

            if serialization not in self._serializers:
                raise Exception(f"Invalid serializer {serialization}, can't encode.")

            content_type, content_encoding, serializer = self._serializers[
                serialization
            ]
            payload = serializer.encode(data, **kwargs)

        else:
            # Make a best guess based on data type
            if isinstance(data, bytes):
                content_type = CONTENT_TYPE_DATA
                content_encoding = "binary"
                payload = data

            elif isinstance(data, str):
                content_type = CONTENT_TYPE_TEXT
                content_encoding = "utf-8"
                payload = data.encode("utf-8")

            else:
                # Use the default encoder
                content_type, content_encoding, serializer = self._serializers[
                    self._default_codec
                ]
                payload = serializer.encode(data)

        return content_type, content_encoding, payload

    def loads(
        self, data: bytes, content_type: str, content_encoding: str, **kwargs
    ) -> Any:
        """ Decode serialized data.

        Deserialize a data blob that was serialized using `dumps` based on `content_type`.

        :param data: The message data to deserialize.

        :param content_type: The content-type of the data (e.g., application/json).

        :param content_encoding: The content-encoding of the data. (e.g., utf-8,
          binary). NOTE: This parameter is not currently used.

        Keywords:

          :param protobuf_id: An integer that uniquely identifies the a
            registered the Protobuf message.

        Raises:
            Exception: If the serialization method requested is not available.
        Returns:
            The deserialized data.
        """
        content_type = content_type if content_type else CONTENT_TYPE_DATA

        # Currently the implementation only supports text data (text, json,
        # yaml) as utf-8. If/when more is needed then the content_encoding
        # parameter will need to be fed down into the serializers.
        # content_encoding = (content_encoding or "utf-8").lower()

        if data:

            serialization = self.type_to_name[content_type]
            if serialization not in self._serializers:
                raise Exception(
                    f"Invalid serializer {serialization} for {content_type}, can't decode."
                )

            _ct, _ce, serializer = self._serializers[serialization]
            return serializer.decode(data, **kwargs)

        return data


def register_none(registry: SerializerRegistry):
    """ The serialization you have when you don't want serialization. """

    class NoneSerializer(ISerializer):
        def encode(self, data, **kwargs):
            """ Returns serialized data as a bytes object. """
            if not isinstance(data, bytes):
                raise Exception(f"Can only serialize bytes type, got {type(data)}")
            return data

        def decode(self, data, **kwargs):
            """ Returns deserialized data """
            return data

    serializer = NoneSerializer()
    registry.register(
        None, serializer, content_type=CONTENT_TYPE_DATA, content_encoding="binary"
    )


def register_text(registry: SerializerRegistry) -> None:
    """ Register an encoder/decoder for TEXT serialization. """

    class TextSerializer(ISerializer):
        def encode(self, text: str, **kwargs) -> bytes:
            """ Encode a string and return a :class:`bytes` object.

            :returns: a serialized message as a bytes object.
            """
            return text.encode("utf-8")

        def decode(self, data: bytes, **kwargs) -> str:
            """ Decode *data* from :class:`bytes` to the original data structure.

            :param data: a bytes object containing a serialized message.

            :returns: A str object.
            """
            return data.decode("utf-8")

    serializer = TextSerializer()
    registry.register(
        "text", serializer, content_type=CONTENT_TYPE_TEXT, content_encoding="utf-8"
    )


def register_json(registry: SerializerRegistry) -> None:
    """ Register an encoder/decoder for JSON serialization. """

    class JsonSerializer(ISerializer):
        def encode(self, data: Any, **kwargs) -> bytes:
            """ Encode an object into JSON and return a :class:`bytes` object.

            :returns: a serialized message as a bytes object.
            """
            return json.dumps(data).encode("utf-8")

        def decode(self, data: bytes, **kwargs) -> str:
            """ Decode *data* from :class:`bytes` to the original data structure.

            :param data: a bytes object containing a serialized message.

            :returns: A Python object.
            """
            data = data if isinstance(data, str) else data.decode("utf-8")
            return json.loads(data)

    serializer = JsonSerializer()
    registry.register(
        "json", serializer, content_type=CONTENT_TYPE_JSON, content_encoding="utf-8"
    )


def register_msgpack(registry: SerializerRegistry) -> None:
    """ Register an encoder/decoder for MsgPack serialization. """

    if have_msgpack:
        from msgpack import packb, unpackb

        class MsgpackSerializer(ISerializer):
            """
            Must use the use_bin_type flag to ensure that str objects
            are returned back as str objects. This avoids the well
            known problem of msgpack 'raw' which returns str and bytes
            objects as bytes.
            """

            def encode(self, data: Any, **kwargs) -> bytes:
                """ Encode an object into MsgPack and return a :class:`bytes` object.

                :returns: a serialized message as a bytes object.
                """
                return msgpack.packb(data, use_bin_type=True)

            def decode(self, data: bytes, **kwargs) -> str:
                """ Decode *data* from :class:`bytes` to the original data structure.

                :param data: a bytes object containing a serialized message.

                :returns: A Python object.
                """
                return msgpack.unpackb(data, raw=False)

        serializer = MsgpackSerializer()
        registry.register(
            "msgpack",
            serializer,
            content_type=CONTENT_TYPE_MSGPACK,
            content_encoding="binary",
        )


def register_yaml(registry: SerializerRegistry) -> None:
    """ Register an encoder/decoder for YAML serialization.

    It is slower than JSON, but allows for more data types to be serialized.
    Useful if you need to send data such as dates.
    """

    if have_yaml:

        class YamlSerializer(ISerializer):
            def encode(self, data: Any, **kwargs) -> bytes:
                """ Encode an object into YAML and return a :class:`bytes` object.

                :returns: a serialized message as a bytes object.
                """
                return yaml.safe_dump(data).encode("utf-8")

            def decode(self, data: bytes, **kwargs) -> str:
                """ Decode *data* from :class:`bytes` to the original data structure.

                :param data: a bytes object containing a serialized message.

                :returns: A Python object.
                """
                return yaml.safe_load(data.decode("utf-8"))

        serializer = YamlSerializer()
        registry.register(
            "yaml", serializer, content_type=CONTENT_TYPE_YAML, content_encoding="utf-8"
        )


def register_avro(registry: SerializerRegistry, schema_registry=None):
    """ Register an encoder/decoder for Apache Avro serialization. """

    if have_avro:

        import avro.io
        import avro.schema

        class SchemaRegistry(object):
            def __init__(self):
                self.id2schema = {}  # type: Dict[int, avro.schema.Schema]
                self._id = 0

            def register_message(self, obj: dict, type_identifier: int = None) -> int:
                """
                :param obj: A message object to register.

                :param type_identifier: An optional message type identifier to
                  use for the object. If not specified then a number will be
                  automatically assigned.
                """
                if isinstance(obj, dict):
                    avro_schema = avro.schema.SchemaFromJSONData(
                        obj, avro.schema.Names()
                    )
                else:
                    avro_schema = obj

                if type_identifier is None:
                    self._id += 1
                    type_identifier = self._id

                self.id2schema[type_identifier] = avro_schema
                return type_identifier

            def get_schema_by_id(self, schema_identifier: int) -> int:
                return self.id2schema[schema_identifier]

        class AvroSerializer(ISerializer):
            def __init__(self, schema_registry=None):
                """
                :param schema_registry: A avro.schema.Schema object populated with the
                  schemas that will be used.
                """
                self.registry = schema_registry if schema_registry else SchemaRegistry()

            def encode(self, data, *, type_identifier: int = None, **kwargs):
                """ Encode an object into Avro and return a :class:`bytes` object.

                :returns: a serialized message as a bytes object.
                """
                schema = self.registry.get_schema_by_id(type_identifier)
                bytes_writer = io.BytesIO()
                encoder = avro.io.BinaryEncoder(bytes_writer)
                datum_writer = avro.io.DatumWriter(schema)
                datum_writer.write(data, encoder)
                return bytes_writer.getvalue()

            def decode(self, data, *, type_identifier: int = None, **kwargs):
                """ Decode *data* from :class:`bytes` to the original data structure.

                :param data: a bytes object containing a serialized message.

                :param type_identifier: An integer specifying the identity of a
                  registered Avro schema. If specified the schema name is used to
                  lookup the schema in a schema registry.

                :returns: A Python object.
                """
                schema = self.registry.get_schema_by_id(type_identifier)
                bytes_reader = io.BytesIO(data)
                decoder = avro.io.BinaryDecoder(bytes_reader)
                datum_reader = avro.io.DatumReader(schema)
                return datum_reader.read(decoder)

        serializer = AvroSerializer(schema_registry=schema_registry)
        registry.register(
            "avro",
            serializer,
            content_type=CONTENT_TYPE_AVRO,
            content_encoding="binary",
        )


def register_protobuf(registry: SerializerRegistry, object_registry=None) -> None:
    """ Register an encoder/decoder for Google Protocol Buffers serialization. """

    if have_protobuf:

        from google.protobuf.message import Message
        from google.protobuf import symbol_database

        class ObjectRegistry(object):
            def __init__(self):
                self.symDb = symbol_database.Default()
                self.id2sym = {}
                self._id = 0

            def register_message(
                self, obj: Message, type_identifier: int = None
            ) -> int:
                """
                :param obj: A message object to register.

                :param type_identifier: An optional message type identifier to
                  use for the object. If not specified then a number will be
                  automatically assigned.
                """
                symbol_name = obj.DESCRIPTOR.name
                if type_identifier is None:
                    self._id += 1
                    type_identifier = self._id
                self.id2sym[type_identifier] = symbol_name
                return type_identifier

            def get_object_by_id(self, type_identifier: int) -> str:
                symbol_name = self.id2sym.get(type_identifier)
                messageClass = self.symDb.GetSymbol(symbol_name)
                return messageClass()

        class ProtobufSerializer(ISerializer):
            """ Google Protocol Buffers serialization.

            When you parse a serialized protocol buffer message you have to know
            what kind of type you're expecting. However, a serialized protocol
            buffer message does not provide this identifying information.

            This becomes a problem when you want to be able to parse different
            message types from a single stream or file.

            One way to accomplish this is to put all the message types inside a
            OneOf field in a top level wrapper message. This is probably the
            simplest option if you have control of the system (the senders, the
            receivers and the .proto definitions).

            If you can't do this then you need to implement a mechanism that can
            supply the type hint information about the original object type in
            order to decode the data. The type hint must be transferred with the
            serialized message data, typically in a message frame header.

            This serializer relies on an object registry to manage the associated
            between an identifier and an object. The identifier is used as a
            lookup key to construct the message class object from the symbol
            database and decode data into it.

            When a pb2 file is imported it automatically adds the messages to the
            default symbol database.
            """

            def __init__(self, object_registry=None):
                """
                :param object_registry: An object that is responsible for translating
                  a Protocol Buffers object to a type identifier and back.
                """
                self.registry = object_registry if object_registry else ObjectRegistry()

            def encode(self, obj: Message, **kwargs) -> bytes:
                """ Encode the given object and return a :class:`bytes` object.

                :param obj: A Protobuf object to serialize into bytes.

                :returns: a serialized message as a bytes object.
                """
                return obj.SerializeToString()

            def decode(self, data: bytes, type_identifier: str, **kwargs) -> Message:
                """ Decode *data* from :class:`bytes` to the original data structure.

                :param data: a bytes object containing a serialized message.

                :param type_identifier: An integer that can be used to uniquely identify
                  the Protobuf message. The identifier is used to find the matching
                  class object and instantiate it. The data is then decoded into the
                  new message instance.

                :returns: A Protobuf message object.

                :raises: KeyError if matching symbol type is not found.
                """
                try:
                    obj = self.registry.get_object_by_id(type_identifier)
                except KeyError:
                    raise Exception(
                        f"Unable to load '{type_identifier}' from symbol database"
                    ) from None
                obj.ParseFromString(data)
                return obj

        serializer = ProtobufSerializer(object_registry=object_registry)
        registry.register(
            "protobuf",
            serializer,
            content_type=CONTENT_TYPE_PROTOBUF,
            content_encoding="binary",
        )


def initialize(registry: SerializerRegistry):
    """ Register serialization methods and set a default """
    register_none(registry)
    register_text(registry)
    register_json(registry)
    register_msgpack(registry)
    register_yaml(registry)
    register_avro(registry)
    register_protobuf(registry)

    registry._set_default_serializer("json")


"""
.. data:: registry

Global registry of serializers/deserializers.
"""
registry = SerializerRegistry()

dumps = registry.dumps

loads = registry.loads

initialize(registry)
