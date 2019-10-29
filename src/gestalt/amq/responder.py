"""
This module contains an AMQP messaging component called a responder, which
forms part of the request/response remote procedure call pattern.
"""

import asyncio
import logging
import inspect
import time
import uuid

from aio_pika import ExchangeType, IncomingMessage, Message, connect_robust
from aio_pika.exceptions import AMQPError
from gestalt.amq import utils
from gestalt.serialization import loads

from asyncio import AbstractEventLoop
from aio_pika import Connection, Channel, Exchange, Queue
from typing import Any, Callable, Dict, Optional

MessageHandlerType = Callable[[Any, IncomingMessage], None]

logger = logging.getLogger(__name__)


class Responder(object):
    """
    This object is used as the server in the Request/Response communications
    pattern. This object binds to a queue it creates to handle service
    requests from clients. Upon receipt of each service request message the
    message is passed to a handler function and the returned object is sent
    back to the client's response queue specified in each message's 'reply_to'
    header field.
    """

    def __init__(
        self,
        amqp_url: str = "",
        exchange_name: str = "",
        exchange_type: ExchangeType = ExchangeType.DIRECT,
        service_name: str = "",
        reconnect_interval: float = 1.0,
        prefetch_count: int = 1,
        serialization: str = None,
        compression: str = None,
        dlx_name: str = "rpc.dlx",
        on_request: Optional[MessageHandlerType] = None,
        loop: AbstractEventLoop = None,
    ) -> None:
        """
        :param amqp_url: The AMQP URL defining the connection parameters.
          Default setting is suitable for connecting to a RabbitMQ container.

        :param exchange_name: The name of the exchange to bind the service
          queue to. This defaults to an empty string which results in the
          default exchange being used.

        :param exchange_type: The type of exchange to declare. Default is
          direct.

        :param service_name: The name to give to this service's message queue.

        :param prefetch_count: This parameter sets the limit of how many
          unacknowledged messages can be outstanding at any time on the
          channel.

        :param serialization: The name of the default serialization strategy to
          use when sending messages. This strategy will be applied if no
          serialization is explicitly specified when sending a message.

        :param compression: An optional string specifying the compression
          strategy to use. It can be provided using the convenience name or
          the mime-type. This strategy will be applied if no compression is
          explicitly specified when sending a message.

        :param dlx_name: The name of a Dead Letter Exchange used by a service
          provider as the destination for sending unhandled messages. The
          same exchange name must be used by client and server as the client
          binds a queue to obtain unhandled messages.

        :param on_request: a user function that will be called whenever a new
          request message is received. The callback is expected to take two
          arguments which are a message payload (automatically decompressed
          and deserialized) and a IncomingMessage object which provides the
          handler function with access to message headers. The function is
          expected to return a response object that will be returned to the
          sender.

        :param loop: The event loop to run in.

        """
        self.loop = loop or asyncio.get_event_loop()
        self.amqp_url = amqp_url if amqp_url else utils.build_amqp_url()
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.service_name = service_name
        self.serialization = serialization
        self.compression = compression
        if on_request is None:
            raise Exception("A response handler must be provided")
        self._request_handler = on_request  # type: MessageHandlerType

        self.reconnect_interval = reconnect_interval
        self.prefetch_count = prefetch_count
        self.connection = None  # type: Optional[Connection]
        self.channel = None  # type: Optional[Channel]
        self.exchange = None  # type: Optional[Exchange]
        self.dlx_name = dlx_name
        self.queue = None  # type: Optional[Queue]

        self._consumer_tag = None  # type: Optional[str]

    async def start(self) -> None:
        """ Start the client """
        if self.queue:
            # Already running
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

        # Specify the maximum number of messages being processed.
        await self.channel.set_qos(prefetch_count=self.prefetch_count)

        # Declare the exchange.
        if self.exchange_name == "":
            self.exchange = self.channel.default_exchange
        else:
            self.exchange = await self.channel.declare_exchange(
                self.exchange_name, self.exchange_type, durable=True
            )

        # Declare the service queue and configure it to send any unhandled
        # messages to a specific dead letter exchange.
        self.queue = await self.channel.declare_queue(
            self.service_name,
            durable=False,
            auto_delete=True,
            arguments={"x-dead-letter-exchange": self.dlx_name},
        )

        if self.exchange_name != "":
            # Bind the queue to the exchange
            logger.debug(
                f"binding responder queue to exchange {self.exchange_name} with routing-key={self.service_name}"
            )
            assert self.queue is not None
            await self.queue.bind(self.exchange, routing_key=self.service_name)

        self._consumer_tag = await self.queue.consume(self._on_request_message)

    async def stop(self) -> None:
        """ Stop the client """
        # Stop the message queue processing task
        if self.queue:
            assert self._consumer_tag is not None
            await self.queue.cancel(self._consumer_tag)
            self._consumer_tag = None
            await self.queue.delete()
        self.queue = None

        if self.channel and not self.channel.is_closed:
            await self.channel.close()
        self.channel = None

        if self.connection and not self.connection.is_closed:
            await self.connection.close()
        self.connection = None

        self.exchange = None

    async def _on_request_message(self, message: IncomingMessage):
        """ Process a request message """

        if not message.reply_to:
            logger.warning(f"Received message without 'reply_to' header. {message}")
            await message.ack()
            return

        try:
            payload = utils.decode_message(message)
        except Exception as exc:
            logger.exception("Problem in message decode function")
            await message.reject(requeue=False)
            return

        try:
            assert self._request_handler is not None
            response = self._request_handler(payload, message)
            if inspect.isawaitable(response):
                response = await response  # type: ignore
        except Exception as exc:
            logger.exception(f"Problem in user message handler function")
            await message.reject(requeue=False)
            return

        headers = {}  # type: Dict[str, str]

        try:
            payload, content_type, content_encoding = utils.encode_payload(
                response,
                content_type=self.serialization,
                compression=self.compression,
                headers=headers,
            )
        except Exception as exc:
            logger.exception("Error encoding response payload")
            await message.reject(requeue=False)
            return

        response_message = Message(
            body=payload,
            content_type=content_type,
            content_encoding=content_encoding,
            timestamp=time.time(),
            correlation_id=message.correlation_id,
            delivery_mode=message.delivery_mode,  # type: ignore
            headers=headers,
        )

        try:
            # Responses should always use the default exchange to route the
            # message to the queue created by the consumer.
            assert self.channel is not None
            await self.channel.default_exchange.publish(
                response_message, message.reply_to, mandatory=False
            )
        except Exception:
            logger.exception(f"Failed to send response: {response_message}")
            await message.reject(requeue=False)
            return

        await message.ack()

    def _on_reconnected(self):
        logger.debug("Reconnected to broker!")

    def _on_channel_closed(self, reason):
        logger.debug(f"Channel closed. Reason: {reason}")
