"""
This module contains an AMQP message producer.
"""

import asyncio
import logging
import time

from aio_pika import ExchangeType, Message, connect_robust
from aio_pika.exceptions import AMQPError
from gestalt.amq import utils

from aio_pika import Channel, Connection, Exchange
from asyncio import AbstractEventLoop
from typing import Any, Dict, Optional


logger = logging.getLogger(__name__)


class Producer:
    """ The Producer publishes messages to RabbitMQ using a topic exchange.
    """

    def __init__(
        self,
        amqp_url: str = "",
        exchange_name: str = "amq.topic",
        exchange_type: ExchangeType = ExchangeType.TOPIC,
        routing_key: str = "",
        reconnect_interval: float = 1.0,
        serialization: str = None,
        compression: str = None,
        loop: AbstractEventLoop = None,
    ) -> None:
        """
        :param amqp_url: A URL defining the AMQP connection parameters.
          If no URL is specified then a default setting is used which is
          suitable for connecting to RabbitMQ on localhost on port 5672,
          with a default username and password of guest and guest and a
          default virtual host of /.

        :param exchange_name: The name of the exchange to publish messages to.

        :param exchange_type: The type of exchange to declare. Default is topic.

        :param routing-key: The default routing key to use when publishing a
          message.

        :param reconnect_interval: The number of seconds between reconnection
          attempts. Defaults to 1.0.

        :param serialization: The name of the default serialization strategy to
          use when publishing messages. This strategy will be applied if no
          serialization is explicitly specified when publishing a message.

        :param compression: An optional string specifying the compression
          strategy to use. It can be provided using the convenience name or
          the mime-type. This strategy will be applied if no compression is
          explicitly specified when publishing a message.

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
        self.connection = None  # type: Optional[Connection]
        self.channel = None  # type: Optional[Channel]
        self.exchange = None  # type: Optional[Exchange]

    async def start(self) -> None:
        """ Start the publisher """
        if self.channel:
            # Already started
            return

        try:
            self.connection = await connect_robust(
                self.amqp_url,
                reconnect_interval=self.reconnect_interval,
                add_reconnect_callback=self._on_reconnected,
            )
        except asyncio.CancelledError:
            logger.info(f"Connection({self.amqp_url}) cancelled")
            return
        except (AMQPError, ConnectionError) as exc:
            logger.error(f"Connection({self.amqp_url}) {exc}")
            raise Exception(f"Can't connect to {self.amqp_url}") from exc

        assert self.connection is not None
        self.channel = await self.connection.channel()
        assert self.channel is not None
        self.channel.add_close_callback(self._on_channel_closed)
        self.exchange = await self.channel.declare_exchange(
            self.exchange_name,
            self.exchange_type,
            durable=self.exchange_name == "amq.topic",
        )

    async def stop(self) -> None:
        """ Stop the publisher """
        if self.channel and not self.channel.is_closed:
            await self.channel.close()
        self.channel = None

        if self.connection and not self.connection.is_closed:
            await self.connection.close()
        self.connection = None

        self.exchange = None

    async def publish_message(
        self,
        data: Any,
        routing_key: str = None,
        content_type: str = None,
        compression: str = None,
        headers: Dict = None,
        type_identifier: int = None,
    ):
        """ Publish a message.

        :param data: The message data to transfer.

        :param routing-key: A routing key to use when publishing the message.
          The routing key determines which queues the exchange passes the
          message to. If not specified then the default routing key provided
          to the class initializer is used.

        :param content_type: A string defining the message serialization
          content type. If not specified then the default serialization
          strategy will be assumed.

        :param compression: The name of the compression strategy to use. The
          default value is None. In this case the default compression strategy
          will be used. If that is None then no compression is used.

        :param headers: Arbitrary headers to pass along with message.

        :param type_identifier: An integer that uniquely identifies a
          registered message. This parameter is only needed for some
          serialization methods that do not code in type awareness, such
          as Avro and Protobuf.
        """
        if self.connection is None:
            logger.error("Producer does not have a connection")
            return

        routing_key = routing_key if routing_key else self.routing_key

        serialization = content_type if content_type else self.serialization
        compression = compression if compression else self.compression

        headers = {}

        try:
            payload, content_type, content_encoding = utils.encode_payload(
                data,
                content_type=serialization,
                compression=compression,
                headers=headers,
                type_identifier=type_identifier,
            )
        except Exception:
            logger.exception("Error encoding payload")
            return

        assert self.exchange is not None
        await self.exchange.publish(
            Message(
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

    def _on_channel_closed(self, reason: Exception):
        logger.debug(f"Channel closed. Reason: {reason}")
