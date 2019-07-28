import asyncio
import datetime
import logging
from gestalt.serialization import CONTENT_TYPE_PROTOBUF
from gestalt.stream.mti import MtiStreamClient
from position_pb2 import Position


if __name__ == "__main__":

    import argparse
    from gestalt.runner import run

    ARGS = argparse.ArgumentParser(description="Stream MTI Client Example")
    ARGS.add_argument(
        "--host",
        metavar="<host>",
        type=str,
        default="localhost",
        help="The host the server will run on",
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

    def on_started(client):
        print("Client has started")

    def on_stopped(client):
        print("Client has stopped")

    def on_peer_available(client, peer_id):
        print(f"Client {peer_id} connected")

        # Upon connection, send a message to the server
        protobuf_data = Position(
            latitude=130.0, longitude=-30.0, altitude=50.0, status=Position.SIMULATED
        )
        print(f"sending a message: {protobuf_data}")
        client.send(protobuf_data, peer_id=peer_id, type_identifier=type_identifier)

    def on_peer_unavailable(client, peer_id):
        print(f"Client {peer_id} connected")

    async def on_message(client, data, peer_id, **kwargs) -> None:
        print(f"Client received msg from {peer_id}: {data}")

        # Wait briefly before sending a reply to the reply!
        await asyncio.sleep(1)

        msg_count = data["counter"] + 1
        # Send a reply to the specific peer that sent the msg
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        msg = dict(timestamp=now.isoformat(), counter=msg_count)
        client.send(msg, peer_id=peer_id)

    svr = MtiStreamClient(
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
