import asyncio
import asynctest
import unittest.mock
from gestalt import serialization
from gestalt.amq.consumer import Consumer
from gestalt.amq.producer import Producer
from gestalt.compression import COMPRESSION_GZIP
from gestalt.serialization import (
    CONTENT_TYPE_AVRO,
    CONTENT_TYPE_JSON,
    CONTENT_TYPE_PROTOBUF,
    CONTENT_TYPE_TEXT,
)

try:
    import aio_pika

    have_pika = True
except ImportError:
    have_pika = False


RABBITMQ_AVAILABLE = False
AMQP_URL = "amqp://guest:guest@localhost:5672/"
POSITION_DICT = dict(latitude=130.0, longitude=-30.0, altitude=50.0)


@unittest.skipUnless(have_pika, "aio_pika is not available")
def setUpModule():
    """ Check RabbitMQ service is available """

    async def is_rabbitmq_available():
        global RABBITMQ_AVAILABLE  # pylint: disable=global-statement
        try:
            conn = aio_pika.Connection(AMQP_URL)
            await conn.connect(timeout=0.1)
            await conn.close()
            RABBITMQ_AVAILABLE = True
        except Exception:
            pass

    loop = asyncio.new_event_loop()
    loop.run_until_complete(is_rabbitmq_available())
    loop.close()


class RabbitmqTopicPubSubTestCase(asynctest.TestCase):
    def setUp(self):
        if not RABBITMQ_AVAILABLE:
            self.skipTest("RabbitMQ service is not available")

    async def test_start_stop_consumer_twice(self):
        """ check calling start and stop twice on consumer """

        con = Consumer(amqp_url=AMQP_URL, routing_key="position.#")
        await con.start()
        await con.start()
        await con.stop()
        await con.stop()

    async def test_start_stop_producer_twice(self):
        """ check calling start and stop twice on producer """

        pro = Producer(amqp_url=AMQP_URL, routing_key="position.update")
        await pro.start()
        await pro.start()
        await pro.stop()
        await pro.stop()

    async def test_topic_pubsub_no_consumer_handler(self):
        """ check consumer drops messages when no handler supplied """
        p = Producer(
            amqp_url=AMQP_URL,
            routing_key="position.update",
            serialization=CONTENT_TYPE_TEXT,
        )

        c = Consumer(amqp_url=AMQP_URL, routing_key="position.#")

        try:
            await p.start()
            await c.start()

            text_msg = "This is a test message"
            await p.publish_message(text_msg)
            await asyncio.sleep(0.1)

        finally:
            await c.stop()
            await p.stop()

    async def test_topic_pubsub_text(self):
        """ check text messages can be published and received """
        exchange_name = "test"

        p = Producer(
            amqp_url=AMQP_URL,
            exchange_name=exchange_name,
            routing_key="position.update",
        )

        on_message_mock = unittest.mock.Mock()
        c = Consumer(
            amqp_url=AMQP_URL,
            exchange_name=exchange_name,
            routing_key="position.#",
            on_message=on_message_mock,
        )

        try:
            await p.start()
            await c.start()

            text_msg = ",".join([f"{k}={v}" for k, v in POSITION_DICT.items()])
            await p.publish_message(
                text_msg, content_type=CONTENT_TYPE_TEXT, compression=COMPRESSION_GZIP
            )

            await asyncio.sleep(0.1)

            self.assertTrue(on_message_mock.called)
            args, _kwargs = on_message_mock.call_args_list[0]
            payload, message = args
            self.assertEqual(payload, text_msg)
            self.assertEqual(message.properties.content_type, CONTENT_TYPE_TEXT)
            self.assertEqual(message.headers.get("compression"), COMPRESSION_GZIP)

        finally:
            await c.stop()
            await p.stop()

    async def test_topic_pubsub_json(self):
        """ check JSON messages can be published and received """
        exchange_name = "test"

        p = Producer(
            amqp_url=AMQP_URL,
            exchange_name=exchange_name,
            routing_key="position.update",
        )

        on_message_mock = unittest.mock.Mock()
        c = Consumer(
            amqp_url=AMQP_URL,
            exchange_name=exchange_name,
            routing_key="position.#",
            on_message=on_message_mock,
        )

        try:
            await p.start()
            await c.start()

            await p.publish_message(
                POSITION_DICT,
                content_type=CONTENT_TYPE_JSON,
                compression=COMPRESSION_GZIP,
            )

            await asyncio.sleep(0.1)

            self.assertTrue(on_message_mock.called)
            args, _kwargs = on_message_mock.call_args_list[0]
            payload, message = args
            self.assertEqual(payload, POSITION_DICT)
            self.assertEqual(message.properties.content_type, CONTENT_TYPE_JSON)
            self.assertEqual(message.headers.get("compression"), COMPRESSION_GZIP)

        finally:
            await c.stop()
            await p.stop()

    @unittest.skipUnless(serialization.have_protobuf, "requires google protobuf")
    async def test_topic_pubsub_protobuf(self):
        """ check Protobuf messages can be published and received """
        from position_pb2 import Position

        exchange_name = "test"

        p = Producer(
            amqp_url=AMQP_URL,
            exchange_name=exchange_name,
            routing_key="position.update",
        )

        on_message_mock = unittest.mock.Mock()
        c = Consumer(
            amqp_url=AMQP_URL,
            exchange_name=exchange_name,
            routing_key="position.#",
            on_message=on_message_mock,
        )

        # Register messages that require using the x-type-id message attribute
        serializer = serialization.registry.get_serializer(CONTENT_TYPE_PROTOBUF)
        _type_identifier = serializer.registry.register_message(Position)

        try:
            await p.start()
            await c.start()

            await asyncio.sleep(0.1)

            protobuf_msg = Position(**POSITION_DICT)
            await p.publish_message(protobuf_msg, content_type=CONTENT_TYPE_PROTOBUF)

            await asyncio.sleep(0.1)

            self.assertTrue(on_message_mock.called)
            args, _kwargs = on_message_mock.call_args_list[0]
            payload, message = args
            self.assertEqual(payload, protobuf_msg)
            self.assertEqual(message.properties.content_type, CONTENT_TYPE_PROTOBUF)
            self.assertEqual(message.headers.get("compression"), None)

        finally:
            await c.stop()
            await p.stop()

    @unittest.skipUnless(serialization.have_avro, "requires avro")
    async def test_topic_pubsub_avro(self):
        """ check Apache Avro messages can be published and received """

        exchange_name = "test"

        p = Producer(
            amqp_url=AMQP_URL,
            exchange_name=exchange_name,
            routing_key="position.update",
        )

        on_message_mock = unittest.mock.Mock()
        c = Consumer(
            amqp_url=AMQP_URL,
            exchange_name=exchange_name,
            routing_key="position.#",
            on_message=on_message_mock,
        )

        # Add schema to serializer schema registry
        message_schema = {
            "namespace": "unittest.test_amq_topic_pubsub",
            "type": "record",
            "name": "Position",
            "fields": [
                {"name": "latitude", "type": ["float", "null"]},
                {"name": "longitude", "type": ["float", "null"]},
                {"name": "altitude", "type": ["float", "null"]},
            ],
        }

        # Register messages that require using the x-type-id message attribute
        serializer = serialization.registry.get_serializer(CONTENT_TYPE_AVRO)
        type_identifier = serializer.registry.register_message(message_schema)

        try:
            await p.start()
            await c.start()

            await asyncio.sleep(0.1)

            avro_msg = POSITION_DICT

            # must provide explicit type-identifier with Avro.
            await p.publish_message(
                avro_msg,
                content_type=CONTENT_TYPE_AVRO,
                type_identifier=type_identifier,
            )

            await asyncio.sleep(0.1)

            self.assertTrue(on_message_mock.called)
            args, _kwargs = on_message_mock.call_args_list[0]
            payload, message = args
            self.assertEqual(payload, avro_msg)
            self.assertEqual(message.properties.content_type, CONTENT_TYPE_AVRO)
            self.assertEqual(message.headers.get("compression"), None)

        finally:
            await c.stop()
            await p.stop()
