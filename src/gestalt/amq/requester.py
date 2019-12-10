"""
This module contains an AMQP messaging component called a requester, which
forms part of the request/response remote procedure call pattern.
"""

import asyncio
import functools
import logging
import time
import uuid
from aio_pika import ExchangeType, connect_robust
from aio_pika.message import DeliveryMode, IncomingMessage, Message, ReturnedMessage
from aio_pika.exceptions import AMQPError, DeliveryError
from gestalt.amq import utils

from asyncio import AbstractEventLoop
from aio_pika import Connection, Channel, Exchange, Queue
from typing import Any, Callable, Dict, Optional, Tuple

MessageHandlerType = Callable[[Any, IncomingMessage], None]

logger = logging.getLogger(__name__)


class Requester:
    """
    This object is used as the initiator of a service call in the
    Request/Response communications pattern. This object sends request messages
    to a service's input queue and waits for the service to send a response to
    a queue set up by this object specifically for responses.

    When the message is sent it is automatically allocated a timeout which is
    encoded into the message header expiration attribute. If the message
    remains in the service queue longer then the expiration time then the
    RabbitMQ broker pops it from the queue and places it in a Dead Letter
    Exchange queue.

    The requester sets up the dead letter queue and subscribes to it so it
    can get back unfulfilled requests.
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
        loop: AbstractEventLoop = None,
    ) -> None:
        """
        :param amqp_url: A URL defining the AMQP connection parameters.
          If no URL is specified then a default setting is used which is
          suitable for connecting to RabbitMQ on localhost on port 5672,
          with a default username and password of guest and guest and a
          default virtual host of /.

        :param exchange_name: The name of the exchange to bind the reply queue
          to. This defaults to an empty string which results in the default
          exchange being used.

        :param exchange_type: The type of exchange to declare. Default is direct.

        :param service_name: The routing-key to use when sending a request.
          This must match the service name queue created by the service
          provider.

        :param prefetch_count: This parameter sets the limit of how many
          unacknowledged messages can be outstanding at any time on the
          channel.

        :param serialization: The name of the default serialization strategy
          to use when sending messages. This strategy will be applied if no
          serialization is explicitly specified when sending a message.

        :param compression: An optional string specifying the compression
          strategy to use. It can be provided using the convenience name or
          the mime-type. This strategy will be applied if no compression is
          explicitly specified when sending a message.

        :param dlx_name: The name of a Dead Letter Exchange used by a service
          provider as the destination for sending unhandled messages. The
          same exchange name must be used by client and server as the client
          binds a queue to obtain unhandled messages.

        :param loop: The event loop to run in.

        """
        self.loop = loop or asyncio.get_event_loop()
        self.amqp_url = amqp_url if amqp_url else utils.build_amqp_url()
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.service_name = service_name
        self.serialization = serialization
        self.compression = compression

        self.reconnect_interval = reconnect_interval
        self.prefetch_count = prefetch_count
        self.connection = None  # type: Optional[Connection]
        self.channel = None  # type: Optional[Channel]
        self.exchange = None  # type: Optional[Exchange]
        self.response_queue = None  # type: Optional[Queue]
        self.dlx_name = dlx_name
        self.dlx_exchange = None  # type: Optional[Exchange]
        self._consumer_tag = None  # type: Optional[str]

        self._response_message_task = None
        self.futures = dict()  # type: Dict[str, asyncio.Future]

    async def start(self) -> None:
        """ Start the requester """
        if self.response_queue:
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

        # Creating a channel
        self.channel = await self.connection.channel()
        assert self.channel is not None
        self.channel.add_close_callback(self._on_channel_closed)
        self.channel.add_on_return_callback(self._on_request_returned)

        # Specify the maximum number of messages being processed.
        await self.channel.set_qos(prefetch_count=self.prefetch_count)

        # Declare the exchange.
        if self.exchange_name == "":
            self.exchange = self.channel.default_exchange
        else:
            self.exchange = await self.channel.declare_exchange(
                self.exchange_name, self.exchange_type, durable=True
            )

        # Create a dead letter exchange where the broker can move any
        # unprocessed requests that were not handled within the nominated
        # expiration time.
        self.dlx_exchange = await self.channel.declare_exchange(
            self.dlx_name, type=ExchangeType.HEADERS, auto_delete=True
        )

        # Declare a queue, which is automatically connected to the default
        # exchange. An exclusive queue is used so that the broker can
        # automatically delete the queue if this consumer disconnects. No
        # name is specified allowing the broker to automatically name the
        # queue.
        self.response_queue = await self.channel.declare_queue(
            exclusive=True, auto_delete=True
        )

        # Bind the response queue to the dead letter exchange so that messages
        # sent there, with certain parameters, also get routed into the
        # response queue for handling.
        await self.response_queue.bind(
            self.dlx_exchange,
            "",
            arguments={"From": self.response_queue.name, "x-match": "any"},
        )

        self._consumer_tag = await self.response_queue.consume(self.on_response_message)

    async def stop(self) -> None:
        """ Stop the requester """
        if self.response_queue:
            assert self._consumer_tag is not None
            await self.response_queue.cancel(self._consumer_tag)
            self._consumer_tag = None

            assert self.dlx_exchange is not None
            await self.response_queue.unbind(
                self.dlx_exchange,
                "",
                arguments={"From": self.response_queue.name, "x-match": "any"},
            )

            # Cancel any oustanding response waiters
            self._discard_pending_responses(reason=asyncio.CancelledError)

            await self.response_queue.delete()
        self.response_queue = None

        if self.channel and not self.channel.is_closed:
            await self.channel.close()
        self.channel = None

        if self.connection and not self.connection.is_closed:
            await self.connection.close()
        self.connection = None

        self.exchange = None

    def __remove_future(self, correlation_id: str, future: asyncio.Future):
        """ Discard a response future from the list of pending responses """
        self.futures.pop(correlation_id, None)

    def create_future(self) -> Tuple[str, asyncio.Future]:
        """ Create a future and a unique correlation identifier.

        The correlation identifier is used to associate a response back to the
        original request.
        """
        correlation_id = str(uuid.uuid4())
        f = self.loop.create_future()
        self.futures[correlation_id] = f
        f.add_done_callback(functools.partial(self.__remove_future, correlation_id))
        return correlation_id, f

    async def request(
        self,
        data: Any,
        *,
        service_name: str = None,
        expiration: int = None,
        delivery_mode: DeliveryMode = DeliveryMode.NOT_PERSISTENT,
    ) -> asyncio.Future:
        """ Send a message to a service and wait for a response.

        :param data: The message data to transfer.

        :param expiration: An optional value representing the number of
          seconds that a message can remain in a queue before being returned
          and a timeout exception (:class:`asyncio.TimeoutError`) is raised.

        :param delivery_mode: Request message delivery mode. Default is
          not-persistent.

        :raises asyncio.TimeoutError: When message expires before being handled.

        :raises CancelledError: when called :func:`RPC.cancel`

        :raises Exception: internal error
        """
        service_name = service_name if service_name else self.service_name

        correlation_id, future = self.create_future()

        headers = {}  # type: Dict[str, str]

        # An exception may be raised here if the message can not be serialized.
        payload, content_type, content_encoding = utils.encode_payload(
            data,
            content_type=self.serialization,
            compression=self.compression,
            headers=headers,
        )

        assert self.response_queue is not None

        # Add a 'From' entry to message headers which will be used to route an
        # expired message to the dead letter exchange queue.
        headers["From"] = self.response_queue.name

        message = Message(
            body=payload,
            content_type=content_type,
            content_encoding=content_encoding,
            timestamp=time.time(),
            correlation_id=correlation_id,
            delivery_mode=delivery_mode,
            reply_to=self.response_queue.name,
            headers=headers,
        )

        if expiration is not None:
            message.expiration = expiration

        logger.debug(
            f"Sending request to {service_name} with correlation_id: {correlation_id}"
        )

        assert self.exchange is not None
        await self.exchange.publish(
            message,
            routing_key=service_name,
            mandatory=True,  # report error if no queues are actively consuming
        )

        logger.debug(f"Waiting for response from {service_name}")
        return await future

    async def on_response_message(self, message: IncomingMessage):
        """ Process a response.

        This method is used to process standard responses as well as requests
        that expired and were sent to the dead letter exchange to which the
        response queue is also linked to.
        """
        await message.ack()

        if message.correlation_id is None:
            logger.warning(f"Message had no correlation_id: {message}")
            return

        f = self.futures.pop(
            message.correlation_id, None
        )  # type: Optional[asyncio.Future]

        if f is None:
            logger.warning(
                f"Unrecognized correlation_id: {message.correlation_id}: {message}"
            )
            return

        if "x-death" in message.headers:
            # The presence of the 'x-death' header indicates the return of an
            # unprocessed request.
            f.set_exception(asyncio.TimeoutError("Message timed-out"))
            return

        try:
            payload = utils.decode_message(message)
        except Exception as e:
            logger.error(f"Failed to deserialize response on message: {message}")
            f.set_exception(e)
            return

        f.set_result(payload)

    def _on_request_returned(self, message: ReturnedMessage):
        """
        Messages sent to a non-existant queue will be returned to this method
        which will raise an exception for the appropriate future that was
        waiting for a response.
        """
        if message.correlation_id is None:
            logger.warning(f"Returned message had no correlation_id: {message}")
            return

        f = self.futures.pop(
            message.correlation_id, None
        )  # type: Optional[asyncio.Future]

        if not f or f.done():
            logger.warning(f"Unknown message was returned: {message}")
            return

        f.set_exception(DeliveryError(message, None))  # type: ignore

    def _on_reconnected(self):
        logger.debug("Reconnected to broker!")

    def _on_channel_closed(self, reason: Exception):
        logger.debug(f"Channel closed. Reason: {reason}")
        self._discard_pending_responses(reason=reason)

    def _discard_pending_responses(self, reason=None):
        for future in self.futures.values():
            if future.done():
                continue
            future.set_exception(reason or Exception)
