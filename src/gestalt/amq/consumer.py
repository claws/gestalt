"""
This module contains an AMQP message consumer.
"""

import asyncio
import logging
import inspect
import time

from aio_pika import ExchangeType, IncomingMessage, connect_robust
from aio_pika.exceptions import AMQPError
from gestalt.amq import utils
from gestalt.serialization import loads

from asyncio import AbstractEventLoop
from aio_pika import Connection, Channel, Exchange, Queue
from typing import Any, Callable, Optional

MessageHandlerType = Callable[[Any, IncomingMessage], None]

logger = logging.getLogger(__name__)


class Consumer(object):
    """ The Consumer subscribes for messages from a topic exchange.

    The consumer creates a fresh queue, letting the server choose a random
    queue to use. This is achieved by not supplying the queue name parameter.

    The consumer instructs the server to delete the queue whenever it
    disconnects. This is achieved by using the 'exclusive' flag.

    """

    def __init__(
        self,
        amqp_url: str = "",
        exchange_name: str = "amq.topic",
        exchange_type: ExchangeType = ExchangeType.TOPIC,
        routing_key: str = "",
        reconnect_interval: float = 1.0,
        prefetch_count: int = 1,
        on_message: MessageHandlerType = None,
        loop: AbstractEventLoop = None,
    ) -> None:
        """
        :param amqp_url: The AMQP URL defining the connection parameters.
          Default setting is suitable for connecting to a RabbitMQ container.

        :param exchange_name: The name of the exchange to publish messages to.

        :param exchange_type: The type of exchange to declare. Default is topic.

        :param routing-key: The routing key to use when binding the message
          queue to the exchange.

        :param reconnect_interval: The number of seconds between reconnection
          attempts. Defaults to 1.0.

        :param prefetch_count: This parameter sets the limit of how many
          unacknowledged messages can be outstanding at any time on the
          channel.

        :param on_message: a user function that will be called whenever a new
          message is received. The callback is expected to take two arguments
          which are a message payload (automatically decompressed and
          deserialized) and a IncomingMessage object which provides
          the handler function with access to message headers.

        :param loop: The event loop to run in.
        """
        self.loop = loop or asyncio.get_event_loop()
        self.amqp_url = amqp_url if amqp_url else utils.build_amqp_url()
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.routing_key = routing_key

        self.reconnect_interval = reconnect_interval
        self.prefetch_count = prefetch_count
        self.connection = None
        self.channel = None
        self.exchange = None
        self.queue = None

        self._consumer_tag = None  # type: Optional[str]
        self._on_message_handler = on_message

    async def start(self) -> None:
        """ Start the client """
        if self.queue:
            # Already started
            return

        try:
            self.connection = await connect_robust(  # type: ignore
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

        # Declare the exchange
        self.exchange = await self.channel.declare_exchange(
            self.exchange_name,
            self.exchange_type,
            durable=self.exchange_name == "amq.topic",
        )

        # Declare a queue. Let the server allocate a queue name and inform the
        # server this queue is exclusively for this consumer. This allows the
        # AMQP broker to delete the queue if this consumer disconnects.
        self.queue = await self.channel.declare_queue(exclusive=True)

        # Bind the queue to the exchange
        logger.debug(
            f"binding consumer queue to exchange {self.exchange_name} with routing-key={self.routing_key}"
        )
        assert self.queue is not None
        await self.queue.bind(self.exchange, routing_key=self.routing_key)

        # Keep a reference to the queue message processing task to so it
        # can be cancelled as part of a graceful shutdown.
        self._consumer_tag = await self.queue.consume(self.on_message)

    async def stop(self) -> None:
        """ Stop the client """
        # Stop the message queue processing task
        if self.queue:
            assert isinstance(self._consumer_tag, str)
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

    async def on_message(self, message: IncomingMessage):
        """ Process a message """
        # Acknowledge that the message has been successfully processed
        # by using a context manager to ACK the message. If an exception
        # occurs in the user callback function then no ACK is sent which
        # returns the message to the queue.
        async with message.process():
            if self._on_message_handler:
                try:
                    payload = utils.decode_message(message)
                except Exception as exc:
                    logger.exception("Problem in message decode function")
                    return

                try:
                    maybe_awaitable = self._on_message_handler(payload, message)
                    if inspect.isawaitable(maybe_awaitable):
                        await maybe_awaitable  # type: ignore
                except Exception as exc:
                    logger.exception(f"Problem in user message handler function")
                    return

    def _on_reconnected(self):
        logger.debug("Reconnected to broker!")

    def _on_channel_closed(self, reason):
        logger.debug(f"Channel closed. Reason: {reason}")
