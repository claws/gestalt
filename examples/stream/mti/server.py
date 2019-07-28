import asyncio
import datetime
import logging
from gestalt.serialization import CONTENT_TYPE_JSON
from gestalt.stream.mti import MtiStreamServer
from position_pb2 import Position


if __name__ == "__main__":

    import argparse
    from gestalt.runner import run

    ARGS = argparse.ArgumentParser(description="Stream MTI Server Example")
    ARGS.add_argument(
        "--host",
        metavar="<host>",
        type=str,
        default="localhost",
        help="The host the server will running on",
    )
    ARGS.add_argument(
        "--port",
        metavar="<port>",
        type=int,
        default=53123,
        help="The port that the server will listen on",
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

    def on_started(server):
        print("Server has started")

    def on_stopped(server):
        print("Server has stopped")

    def on_peer_available(server, peer_id):
        print(f"Server peer {peer_id} connected")

    def on_peer_unavailable(server, peer_id):
        print(f"Server peer {peer_id} connected")

    async def on_message(server, data, peer_id, **kwargs) -> None:
        print(f"Server received msg from {peer_id}: {data}")

        # Wait briefly before sending a reply to the reply!
        await asyncio.sleep(1)

        protobuf_data = Position(
            latitude=130.0, longitude=-30.0, altitude=50.0, status=Position.SIMULATED
        )
        print(f"sending a message: {protobuf_data}")
        client.send(protobuf_data, peer_id=peer_id, type_identifier=type_identifier)

        msg_count = data["counter"] + 1
        server.send(msg, peer_id=peer_id)

    svr = MtiStreamServer(
        on_message=on_message,
        on_started=on_started,
        on_stopped=on_stopped,
        on_peer_available=on_peer_available,
        on_peer_unavailable=on_peer_unavailable,
        content_type=CONTENT_TYPE_JSON,
    )

    run(svr.start(args.host, args.port), finalize=svr.stop)
