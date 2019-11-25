import asyncio
import datetime
import logging
from gestalt.serialization import CONTENT_TYPE_PROTOBUF
from gestalt.stream.mti import MtiStreamServer
from position_pb2 import Position


if __name__ == "__main__":

    import argparse
    from gestalt.runner import run

    parser = argparse.ArgumentParser(description="Stream MTI Server Example")
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

    logging.basicConfig(
        format="%(asctime)s.%(msecs)03.0f [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=getattr(logging, args.log_level.upper()),
    )

    def on_started(server):
        print("Server has started")

    def on_stopped(server):
        print("Server has stopped")

    def on_peer_available(server, peer_id):
        print(f"Server peer {peer_id} connected")

    def on_peer_unavailable(server, peer_id):
        print(f"Server peer {peer_id} disconnected")

    async def on_message(server, data, peer_id, **kwargs) -> None:
        print(f"Server received msg from {peer_id}: {data}")

        # Wait briefly before sending a reply to the reply!
        await asyncio.sleep(1)

        protobuf_data = Position(
            latitude=data.latitude + 0.1,
            longitude=data.longitude + 0.1,
            altitude=data.altitude + 0.1,
            status=Position.SIMULATED,
        )
        print(f"sending a message: {protobuf_data}")
        server.send(protobuf_data, peer_id=peer_id, type_identifier=type_identifier)

    svr = MtiStreamServer(
        on_message=on_message,
        on_started=on_started,
        on_stopped=on_stopped,
        on_peer_available=on_peer_available,
        on_peer_unavailable=on_peer_unavailable,
        content_type=CONTENT_TYPE_PROTOBUF,
    )

    # Associate a message object with a unique message type identifier.
    type_identifier = 1
    svr.register_message(type_identifier, Position)

    run(svr.start(args.host, args.port), finalize=svr.stop)
