import asyncio
import random
from gestalt.compression import COMPRESSION_BZ2, COMPRESSION_GZIP
from gestalt.serialization import (
    CONTENT_TYPE_TEXT,
    CONTENT_TYPE_JSON,
    CONTENT_TYPE_MSGPACK,
    CONTENT_TYPE_PROTOBUF,
    CONTENT_TYPE_YAML,
)
from gestalt import serialization
from gestalt.amq.producer import Producer
from gestalt.runner import run
from position_pb2 import Position


async def message_producer(p: Producer) -> None:
    """ Generate a new Position message, in various formats, and publish it """

    while True:

        msg = dict(latitude=130.0, longitude=-30.0, altitude=50.0)

        content_type = random.choice(
            [
                CONTENT_TYPE_TEXT,
                CONTENT_TYPE_JSON,
                CONTENT_TYPE_MSGPACK,
                CONTENT_TYPE_YAML,
                CONTENT_TYPE_PROTOBUF,
            ]
        )
        compression_name = random.choice([None, COMPRESSION_GZIP, COMPRESSION_BZ2])

        print(
            f"Sending message using content-type={content_type}, compression={compression_name}"
        )

        if content_type == CONTENT_TYPE_TEXT:
            msg = ",".join(f"{k}={v}" for k, v in msg.items())
        elif content_type == CONTENT_TYPE_PROTOBUF:
            msg = Position(**msg)

        await p.publish_message(
            msg, content_type=content_type, compression=compression_name
        )
        await asyncio.sleep(1.0)


if __name__ == "__main__":

    import argparse
    import logging

    parser = argparse.ArgumentParser(description="AMQP Topic Producer Example")
    parser.add_argument(
        "--amqp-url", metavar="<url>", type=str, default=None, help="The AMQP URL"
    )
    parser.add_argument(
        "--exchange-name",
        metavar="<name>",
        type=str,
        default="test",
        help="The AMQP exchange name. Defaults to 'test'",
    )
    parser.add_argument(
        "--routing-key",
        metavar="<pattern>",
        type=str,
        default="position.update",
        help="The routing key to use when publishing messages. Defaults to 'position.update'",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["debug", "info", "error"],
        default="error",
        help="Logging level. Default is 'error'.",
    )

    args = parser.parse_args()

    try:
        numeric_level = getattr(logging, args.log_level.upper())
    except ValueError:
        raise Exception(f"Invalid log-level: {args.log_level}")

    logging.basicConfig(
        format="%(asctime)s.%(msecs)03.0f [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=numeric_level,
    )

    p = Producer(
        amqp_url=args.amqp_url,
        exchange_name=args.exchange_name,
        routing_key=args.routing_key,
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
    serializer = serialization.registry.get_serializer(CONTENT_TYPE_PROTOBUF)
    type_identifier = serializer.registry.register_message(Position)

    async def start_producing(p):
        await p.start()

        # Start producing messages
        loop = asyncio.get_event_loop()
        loop.create_task(message_producer(p))

    run(start_producing(p), finalize=p.stop)
