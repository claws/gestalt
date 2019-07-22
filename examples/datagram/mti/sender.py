import asyncio
import datetime
import logging
from gestalt.serialization import CONTENT_TYPE_PROTOBUF
from gestalt.comms.datagram.mti import MtiDatagramEndpoint
from position_pb2 import Position


if __name__ == "__main__":

    import argparse
    from gestalt.runner import run

    ARGS = argparse.ArgumentParser(description="UDP Sender Example")
    ARGS.add_argument(
        "--host",
        metavar="<host>",
        type=str,
        default="localhost",
        help="The host the receiver is running on",
    )
    ARGS.add_argument(
        "--port",
        metavar="<port>",
        type=int,
        default=53123,
        help="The port that the receiver is listening on",
    )
    ARGS.add_argument(
        "--log-level",
        type=str,
        default="error",
        help="Logging level [debug|info|error]. Default is 'error'.",
    )

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

    s = MtiDatagramEndpoint(content_type=CONTENT_TYPE_PROTOBUF)

    # Associate a message object with a unique message type identifier.
    type_identifier = 1
    s.register_message(type_identifier, Position)

    async def message_producer(s) -> None:
        """ Generate a new message and send it """
        while True:
            protobuf_data = Position(
                latitude=130.0,
                longitude=-30.0,
                altitude=50.0,
                status=Position.SIMULATED,
            )
            print(f"sending a message: {protobuf_data}")
            s.send(protobuf_data, type_identifier=type_identifier)
            await asyncio.sleep(1.0)

    async def start_producing(s, remote_addr):
        await s.start(remote_addr=remote_addr)

        # Start producing messages
        loop = asyncio.get_event_loop()
        loop.create_task(message_producer(s))

    remote_addr = (args.host, args.port)
    run(start_producing(s, remote_addr), finalize=s.stop)
