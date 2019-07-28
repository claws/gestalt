import argparse
import asyncio
import logging
import socket
from gestalt.runner import run
from gestalt.serialization import CONTENT_TYPE_JSON
from gestalt.datagram.endpoint import DatagramEndpoint


if __name__ == "__main__":

    ARGS = argparse.ArgumentParser(description="UDP Receiver Example")
    ARGS.add_argument(
        "--host",
        metavar="<host>",
        type=str,
        default="0.0.0.0",  # socket.gethostbyname(socket.gethostname()),
        help="The host the receiver will running on",
    )
    ARGS.add_argument(
        "--port",
        metavar="<port>",
        type=int,
        default=53123,
        help="The port that the receiver will listening on",
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

    def on_message(data, **kwargs) -> None:
        addr = kwargs.get("addr")
        print(f"Received msg from {addr}: {data}")

    r = DatagramEndpoint(on_message=on_message, content_type=CONTENT_TYPE_JSON)

    local_addr = (args.host, args.port)
    run(r.start(local_addr=local_addr, reuse_port=True), finalize=r.stop)
