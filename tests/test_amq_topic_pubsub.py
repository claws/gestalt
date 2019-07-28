import asyncio
import asynctest
import logging
import unittest.mock
from gestalt import serialization
from gestalt.amq.consumer import Consumer
from gestalt.amq.producer import Producer
from gestalt.compression import COMPRESSION_GZIP, COMPRESSION_BZ2
from gestalt.serialization import (
    CONTENT_TYPE_TEXT,
    CONTENT_TYPE_JSON,
    CONTENT_TYPE_PROTOBUF,
    registry,
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
        global RABBITMQ_AVAILABLE
        try:
            conn = aio_pika.Connection(AMQP_URL)
            await conn.connect(timeout=0.1)
            await conn.close()
            RABBITMQ_AVAILABLE = True
        except Exception as exc:
            pass

    loop = asyncio.get_event_loop()
    loop.run_until_complete(is_rabbitmq_available())
    loop.close()


class RabbitmqTopicPubSubTestCase(asynctest.TestCase):
    def setUp(self):
        if not RABBITMQ_AVAILABLE:
            self.skipTest("RabbitMQ service is not available")

    async def test_topic_pubsub_text(self):
        """ check text messages can be published and received """
        amqp_url = AMQP_URL
        exchange_name = "test"

        p = Producer(
            amqp_url=amqp_url,
            exchange_name=exchange_name,
            routing_key="position.update",
        )

        on_message_mock = unittest.mock.Mock()
        c = Consumer(
            amqp_url=amqp_url,
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
            args, kwargs = on_message_mock.call_args_list[0]
            payload, message = args
            self.assertEqual(payload, text_msg)
            self.assertEqual(message.properties.content_type, CONTENT_TYPE_TEXT)
            self.assertEqual(message.headers.get("compression"), COMPRESSION_GZIP)

        finally:
            await c.stop()
            await p.stop()

    async def test_topic_pubsub_json(self):
        """ check JSON messages can be published and received """
        amqp_url = AMQP_URL
        exchange_name = "test"

        p = Producer(
            amqp_url=amqp_url,
            exchange_name=exchange_name,
            routing_key="position.update",
        )

        on_message_mock = unittest.mock.Mock()
        c = Consumer(
            amqp_url=amqp_url,
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
            args, kwargs = on_message_mock.call_args_list[0]
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

        amqp_url = AMQP_URL
        exchange_name = "test"

        p = Producer(
            amqp_url=amqp_url,
            exchange_name=exchange_name,
            routing_key="position.update",
        )

        on_message_mock = unittest.mock.Mock()
        c = Consumer(
            amqp_url=amqp_url,
            exchange_name=exchange_name,
            routing_key="position.#",
            on_message=on_message_mock,
        )

        # Register messages that require using the x-type-id message attribute
        serializer = serialization.registry.get_serializer("protobuf")
        type_identifier = serializer.registry.register_message(Position)

        try:
            await p.start()
            await c.start()

            await asyncio.sleep(0.1)

            protobuf_msg = Position(**POSITION_DICT)
            await p.publish_message(
                protobuf_msg,
                content_type=CONTENT_TYPE_PROTOBUF,
                type_identifier=type_identifier,
            )

            await asyncio.sleep(0.1)

            self.assertTrue(on_message_mock.called)
            args, kwargs = on_message_mock.call_args_list[0]
            payload, message = args
            self.assertEqual(payload, protobuf_msg)
            self.assertEqual(message.properties.content_type, CONTENT_TYPE_PROTOBUF)
            self.assertEqual(message.headers.get("compression"), None)

        finally:
            await c.stop()
            await p.stop()
