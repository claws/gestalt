import asyncio
import asynctest
import datetime
import logging
import unittest.mock
from gestalt import serialization
from gestalt.amq.requester import Requester
from gestalt.amq.responder import Responder
from gestalt.compression import COMPRESSION_GZIP, COMPRESSION_BZ2
from gestalt.serialization import CONTENT_TYPE_JSON, registry

try:
    import aio_pika

    have_pika = True
except ImportError:
    have_pika = False


RABBITMQ_AVAILABLE = False
AMQP_URL = "amqp://guest:guest@localhost:5672/"


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

    loop = asyncio.new_event_loop()
    loop.run_until_complete(is_rabbitmq_available())
    loop.close()


class RabbitmqRequestReplyTestCase(asynctest.TestCase):
    def setUp(self):
        if not RABBITMQ_AVAILABLE:
            self.skipTest("RabbitMQ service is not available")

    async def test_request_reply_json(self):
        """ check JSON messages can be published and received """
        service_name = "clock"

        req = Requester(
            amqp_url=AMQP_URL,
            service_name=service_name,
            serialization=CONTENT_TYPE_JSON,
        )

        async def on_request(payload, message):
            now = datetime.datetime.now(
                tz=datetime.timezone.utc if payload["utc"] else None
            )
            return dict(timestamp=now.timestamp())

        rep = Responder(
            amqp_url=AMQP_URL,
            service_name=service_name,
            serialization=CONTENT_TYPE_JSON,
            on_request=on_request,
        )

        try:
            await req.start()
            await rep.start()

            request_msg = dict(abc=123, utc=True)
            response_msg = await req.request(request_msg)
            self.assertIn("timestamp", response_msg)

        finally:
            await req.stop()
            await rep.stop()
