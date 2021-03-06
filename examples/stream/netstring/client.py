import asyncio
import datetime
import logging
from gestalt.serialization import CONTENT_TYPE_JSON
from gestalt.stream.netstring import NetstringStreamClient


if __name__ == "__main__":

    import argparse
    from gestalt.runner import run

    parser = argparse.ArgumentParser(description="Stream Netstring Client Example")
    parser.add_argument(
        "--host",
        metavar="<host>",
        type=str,
        default="localhost",
        help="The host the server will run on",
    )
    parser.add_argument(
        "--port",
        metavar="<port>",
        type=int,
        default=53123,
        help="The port that the server will listen on",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["debug", "info", "error"],
        default="error",
        help="Logging level [debug|info|error]. Default is 'error'.",
    )

    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s.%(msecs)03.0f [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=getattr(logging, args.log_level.upper()),
    )

    def on_started(cli: NetstringStreamClient):
        print("Client has started")

    def on_stopped(cli: NetstringStreamClient):
        print("Client has stopped")

    def on_peer_available(cli: NetstringStreamClient, peer_id):
        print(f"Client {peer_id} connected")

        # Upon connection, send a message to the server
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        msg = dict(timestamp=now.isoformat(), counter=1)
        client.send(msg, peer_id=peer_id)

    def on_peer_unavailable(cli: NetstringStreamClient, peer_id):
        print(f"Client {peer_id} connected")

    async def on_message(cli: NetstringStreamClient, data, peer_id, **kwargs) -> None:
        print(f"Client received msg from {peer_id}: {data}")

        # Wait briefly before sending a reply to the reply!
        await asyncio.sleep(1)

        msg_count = data["counter"] + 1
        # Send a reply to the specific peer that sent the msg
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        msg = dict(timestamp=now.isoformat(), counter=msg_count)
        client.send(msg, peer_id=peer_id)

    client = NetstringStreamClient(
        on_message=on_message,
        on_started=on_started,
        on_stopped=on_stopped,
        on_peer_available=on_peer_available,
        on_peer_unavailable=on_peer_unavailable,
        content_type=CONTENT_TYPE_JSON,
    )

    run(client.start(args.host, args.port), finalize=client.stop)
