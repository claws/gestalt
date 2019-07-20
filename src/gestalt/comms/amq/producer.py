"""
This module contains an AMQP message producer.
"""

import asyncio
import logging
import aio_pika
import time
from gestalt.comms.amq import utils

from asyncio import AbstractEventLoop
from typing import Any, Callable, Dict


logger = logging.getLogger(__name__)


class Producer(object):
    """ The Producer publishes messages to RabbitMQ using a topic exchange.
    """

    def __init__(
        self,
        # app_id: str,
        amqp_url: str,
        exchange_name: str = "",
        exchange_type: aio_pika.ExchangeType = aio_pika.ExchangeType.TOPIC,
        routing_key: str = "",
        reconnect_interval: int = 1.0,
        serialization: str = None,
        compression: str = None,
        loop: AbstractEventLoop = None,
    ) -> None:
        """
        :param amqp_url: The AMQP URL.

        :param exchange_name: The name of the exchange to publish messages to.

        :param exchange_type: The type of exchange to declare. Default is topic.

        :param routing-key: The routing key to use when binding the message
          queue to the exchange.

        :param serialization: The name of the default serialization strategy to
          use when publishing messages.

        :param compression: The name of the default compression strategy to
          use when publishing messages.

        :param loop: The event loop to run in. Defaults to the currently
          running event loop.
        """
        self.loop = loop or asyncio.get_event_loop()
        self.amqp_url = amqp_url if amqp_url else utils.build_amqp_url()
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.routing_key = routing_key
        self.serialization = serialization
        self.compression = compression

        self.reconnect_interval = reconnect_interval
        self.connection = None
        self.channel = None
        self.exchange = None

    async def start(self) -> None:
        """ Start the publisher """
        self.connection = await aio_pika.connect_robust(
            self.amqp_url,
            reconnect_interval=self.reconnect_interval,
            add_reconnect_callback=self._on_reconnected,
        )
        self.channel = await self.connection.channel()
        self.channel.add_close_callback(self._on_channel_closed)

        self.exchange = await self.channel.declare_exchange(
            self.exchange_name, self.exchange_type
        )

    async def stop(self) -> None:
        """ Stop the publisher """
        await self.channel.close()
        await self.connection.close()
        self.connection = None
        self.channel = None
        self.exchange = None

    async def publish_message(
        self,
        data: Any,
        routing_key: str = None,
        content_type: str = None,
        compression: str = None,
        headers: Dict = None,
    ):
        """ Publish a message.

        :param data: The message data to transfer.

        :param routing-key: A routing key to use when publishing the message.
          The routing key determines which queues the exchange passes the
          message to. If not specified then the default routing key provided
          to the class initializer is used.

        :param content_type: A string defining the message serialization
          content type. By default the value is None in which case a best effort
          guess will be made based on the supplied data.

        :param compression: The name of the compression strategy to use. The
          default value is None. In this case the default compression strategy
          will be used. If that is None then no compression is used.

        :param headers: Arbitrary headers to pass along with message.

        """
        if self.connection is None:
            logger.error("Producer does not have a connection")
            return

        routing_key = routing_key if routing_key else self.routing_key

        compression = compression if compression else self.compression

        headers = {}

        try:
            payload, content_type, content_encoding = utils.encode_payload(
                data,
                content_type=content_type,
                compression=compression,
                headers=headers,
            )
        except Exception as exc:
            logger.exception("Error encoding payload")
            return

        await self.exchange.publish(
            aio_pika.Message(
                body=payload,
                content_type=content_type,
                content_encoding=content_encoding,
                timestamp=time.time(),
                headers=headers,
            ),
            routing_key=routing_key,
            mandatory=False,  # don't care if no routes are actively consuming
        )

    def _on_reconnected(self):
        logger.debug("Reconnected to broker!")

    def _on_channel_closed(self, reason: str):
        logger.debug(f"Channel closed. Reason: {reason}")
