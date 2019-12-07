import asyncio
import datetime
import logging
from gestalt.serialization import CONTENT_TYPE_JSON
from gestalt.datagram.endpoint import DatagramEndpoint


if __name__ == "__main__":

    import argparse
    from gestalt.runner import run

    parser = argparse.ArgumentParser(description="UDP Sender Example")
    parser.add_argument(
        "--host",
        metavar="<host>",
        type=str,
        default="localhost",
        help="The host the receiver is running on",
    )
    parser.add_argument(
        "--port",
        metavar="<port>",
        type=int,
        default=53123,
        help="The port that the receiver is listening on",
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

    ep = DatagramEndpoint(content_type=CONTENT_TYPE_JSON)

    async def message_producer(e) -> None:
        """ Generate a new message and send it """
        while True:
            now = datetime.datetime.now(tz=datetime.timezone.utc)
            json_msg = dict(timestamp=now.isoformat())
            print(f"sending a message: {json_msg}")
            e.send(json_msg)
            await asyncio.sleep(1.0)

    async def start_producing(e, remote_addr):
        await e.start(remote_addr=remote_addr)

        # Start producing messages
        loop = asyncio.get_event_loop()
        loop.create_task(message_producer(e))

    remote_address = (args.host, args.port)
    run(start_producing(ep, remote_address), finalize=ep.stop)
