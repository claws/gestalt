import asyncio
import datetime
import logging
from gestalt.serialization import CONTENT_TYPE_DATA
from delimited import LineDelimitedStreamServer


if __name__ == "__main__":

    import argparse
    from gestalt.runner import run

    parser = argparse.ArgumentParser(description="Stream Netstring Server Example")
    parser.add_argument(
        "--host",
        metavar="<host>",
        type=str,
        default="localhost",
        help="The host the server will running on",
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

    def on_started(server):
        print("Server has started")

    def on_stopped(server):
        print("Server has stopped")

    def on_peer_available(server, peer_id):
        print(f"Server peer {peer_id} connected")

    def on_peer_unavailable(server, peer_id):
        print(f"Server peer {peer_id} connected")

    async def on_message(server, data, peer_id, **kwargs) -> None:
        msg = data.decode()
        print(f"Server received msg from {peer_id}: {msg}")

        # Wait briefly before sending a reply to the reply!
        await asyncio.sleep(1)

        now = datetime.datetime.now(tz=datetime.timezone.utc)
        msg = now.isoformat()
        # Send a reply to the specific peer that sent the msg
        server.send(msg.encode(), peer_id=peer_id)

    svr = LineDelimitedStreamServer(
        on_message=on_message,
        on_started=on_started,
        on_stopped=on_stopped,
        on_peer_available=on_peer_available,
        on_peer_unavailable=on_peer_unavailable,
        content_type=CONTENT_TYPE_DATA,
    )

    run(svr.start(args.host, args.port), finalize=svr.stop)
