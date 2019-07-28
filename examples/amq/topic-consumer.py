import argparse
import asyncio
import logging
import aio_pika
from gestalt import serialization
from gestalt.amq.consumer import Consumer
from gestalt.runner import run
from position_pb2 import Position
from typing import Any


ARGS = argparse.ArgumentParser(description="AMQP Topic Consumer Example")
ARGS.add_argument(
    "--amqp-url",
    metavar="<url>",
    type=str,
    default="amqp://guest:guest@localhost:5672/",
    help="The AMQP URL",
)
ARGS.add_argument(
    "--exchange-name",
    metavar="<name>",
    type=str,
    default="test",
    help="The AMQP exchange name. Defaults to 'test'",
)
ARGS.add_argument(
    "--routing-key",
    metavar="<pattern>",
    type=str,
    default="position.#",
    help="The routing key to bind the message queue. Defaults to 'position.#'",
)
ARGS.add_argument(
    "--log-level",
    type=str,
    default="error",
    help="Logging level [debug|info|error]. Default is 'error'.",
)


async def on_message_callback(payload: Any, message: aio_pika.IncomingMessage):
    """ Consume messages in various formats.

    :param payload: The original message data after having gone through any
        decompression and deserialization steps.

    :param message: A aio_pika.IncomingMessage object containing meta-data that was
        delivered with the message.
    """
    print(f"\nReceived msg")
    print(f"content_type={message.properties.content_type}")
    print(f"compression: {message.headers.get('compression')}")
    print(f"payload: {payload}")


if __name__ == "__main__":

    args = ARGS.parse_args()

    try:
        numeric_level = getattr(logging, args.log_level.upper())
    except ValueError:
        raise Exception(f"Invalid log-level: {args.log_level}")

    logging.basicConfig(
        format="%(asctime)s.%(msecs)03.0f [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=numeric_level,
    )

    c = Consumer(
        amqp_url=args.amqp_url,
        exchange_name=args.exchange_name,
        routing_key=args.routing_key,
        on_message=on_message_callback,
    )

    # When you parse a serialized protocol buffer message you have to know what
    # kind of type you're expecting. However, a serialized protocol buffer
    # message does not provide this identifying information. When using Protobuf
    # serialization the type must be registered with the serializer so that
    # an identifier can be associated with the type. The type identifier is
    # pass in a message header field. This must be done on sender and consumer
    # sides in the same order.
    #
    # Register messages that require using the x-type-id message attribute
    serializer = serialization.registry.get_serializer(
        serialization.CONTENT_TYPE_PROTOBUF
    )
    type_identifier = serializer.registry.register_message(Position)

    run(c.start, finalize=c.stop)
