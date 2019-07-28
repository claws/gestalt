import argparse
import asyncio
import logging
import random
from gestalt.compression import COMPRESSION_GZIP, COMPRESSION_BZ2
from gestalt.serialization import (
    CONTENT_TYPE_TEXT,
    CONTENT_TYPE_JSON,
    CONTENT_TYPE_PROTOBUF,
)
from gestalt import serialization
from gestalt.amq.producer import Producer
from gestalt.runner import run
from position_pb2 import Position


async def message_producer(p: Producer) -> None:
    """ Generate a new GPS message, in various formats, and publish it """

    LATITUDE = 130.0
    LONGITUDE = -30.0
    ALTITUDE = 50.0

    count = 0
    while True:
        count += 1
        # Apply some pretend jitter to simulate realism
        latitude = LATITUDE + (LATITUDE * 0.01)
        longitude = LONGITUDE + (LONGITUDE * 0.01)
        altitude = ALTITUDE + (ALTITUDE * 0.01)

        text_msg = f"latitude={latitude},longitude={longitude},altitude={altitude}"
        print(f"sending a text message: {text_msg}")
        compression_name = random.choice([COMPRESSION_GZIP, COMPRESSION_BZ2])
        await p.publish_message(
            text_msg, content_type=CONTENT_TYPE_TEXT, compression=compression_name
        )
        await asyncio.sleep(1.0)

        json_msg = dict(latitude=latitude, longitude=longitude, altitude=altitude)
        print(f"sending a JSON message: {json_msg}")
        await p.publish_message(
            json_msg, content_type=CONTENT_TYPE_JSON, compression="zlib"
        )
        await asyncio.sleep(1.0)

        protobuf_msg = Position(
            latitude=latitude, longitude=longitude, altitude=altitude
        )
        print(f"sending a Protobuf message: {protobuf_msg}")
        await p.publish_message(protobuf_msg, content_type=CONTENT_TYPE_PROTOBUF)
        await asyncio.sleep(1.0)


ARGS = argparse.ArgumentParser(description="AMQP Topic Producer Example")
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
    default="position.update",
    help="The routing key to use when publishing messages. Defaults to 'position.update'",
)
ARGS.add_argument(
    "--log-level",
    type=str,
    default="error",
    help="Logging level [debug|info|error]. Default is 'error'.",
)


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
