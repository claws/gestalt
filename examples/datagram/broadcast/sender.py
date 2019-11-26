import argparse
import asyncio
import datetime
import logging
import socket
from gestalt.runner import run
from gestalt.serialization import CONTENT_TYPE_JSON
from gestalt.datagram.endpoint import DatagramEndpoint


if __name__ == "__main__":

    ip_addr = socket.gethostbyname(socket.gethostname())
    # crude guess of the network broadcast address
    broadcast_ip = ip_addr.rsplit(".", 1)[0] + ".255"

    parser = argparse.ArgumentParser(description="UDP Sender Example")
    parser.add_argument(
        "--broadcast-host",
        metavar="<host>",
        type=str,
        default=broadcast_ip,
        help="The network address to broadcast to",
    )
    parser.add_argument(
        "--broadcast-port",
        metavar="<port>",
        type=int,
        default=53123,
        help="The port to broadcast to",
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

    s = DatagramEndpoint(content_type=CONTENT_TYPE_JSON)

    async def message_producer(s, bcast_addr) -> None:
        """ Generate a new message and send it """
        while True:
            now = datetime.datetime.now(tz=datetime.timezone.utc)
            json_msg = dict(timestamp=now.isoformat())
            print(f"sending message: {json_msg}")
            s.send(json_msg, addr=bcast_addr)
            await asyncio.sleep(1.0)

    async def start_producing(s, local_addr, bcast_addr):
        await s.start(local_addr=local_addr, allow_broadcast=True)

        # Start producing messages
        loop = asyncio.get_event_loop()
        loop.create_task(message_producer(s, bcast_addr))

    local_addr = ("0.0.0.0", 0)
    bcast_addr = (args.broadcast_host, args.broadcast_port)
    run(start_producing(s, local_addr, bcast_addr), finalize=s.stop)
