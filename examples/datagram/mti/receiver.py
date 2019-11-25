import asyncio
import logging
from gestalt.serialization import CONTENT_TYPE_PROTOBUF
from gestalt.datagram.mti import MtiDatagramEndpoint
from position_pb2 import Position


if __name__ == "__main__":

    import argparse
    from gestalt.runner import run

    parser = argparse.ArgumentParser(description="UDP Receiver Example")
    parser.add_argument(
        "--host",
        metavar="<host>",
        type=str,
        default="localhost",
        help="The host the receiver will running on",
    )
    parser.add_argument(
        "--port",
        metavar="<port>",
        type=int,
        default=53123,
        help="The port that the receiver will be listening on",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["debug", "info", "error"],
        default="error",
        help="Logging level. Default is 'error'.",
    )

    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s.%(msecs)03.0f [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=getattr(logging, args.log_level.upper()),
    )

    def on_message(self, data, **kwargs) -> None:
        addr = kwargs.get("addr")
        print(f"Received msg from {addr}: {data}")

    r = MtiDatagramEndpoint(on_message=on_message, content_type=CONTENT_TYPE_PROTOBUF)

    # Associate a message object with a unique message type identifier.
    type_identifier = 1
    r.register_message(type_identifier, Position)

    local_addr = (args.host, args.port)
    run(r.start(local_addr=local_addr), finalize=r.stop)
