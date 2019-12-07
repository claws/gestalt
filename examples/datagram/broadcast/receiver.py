import argparse
import logging
from gestalt.runner import run
from gestalt.serialization import CONTENT_TYPE_JSON
from gestalt.datagram.endpoint import DatagramEndpoint


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="UDP Receiver Example")
    parser.add_argument(
        "--host",
        metavar="<host>",
        type=str,
        default="0.0.0.0",
        help="The host the receiver will running on",
    )
    parser.add_argument(
        "--port",
        metavar="<port>",
        type=int,
        default=53123,
        help="The port that the receiver will listening on",
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

    ep = DatagramEndpoint(on_message=on_message, content_type=CONTENT_TYPE_JSON)

    local_address = (args.host, args.port)
    run(ep.start(local_addr=local_address, reuse_port=True), finalize=ep.stop)
