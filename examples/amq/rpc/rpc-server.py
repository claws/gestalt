import argparse
import datetime
import logging
import random
from gestalt.amq.responder import Responder
from gestalt.serialization import CONTENT_TYPE_JSON
from gestalt.runner import run

from aio_pika import IncomingMessage
from typing import Any


logger = logging.getLogger(__name__)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="AMQP RPC Server Example")
    parser.add_argument(
        "--amqp-url", metavar="<url>", type=str, default=None, help="The AMQP URL"
    )
    parser.add_argument(
        "--exchange-name",
        metavar="<name>",
        type=str,
        default="",
        help="The AMQP exchange name. Defaults to a empty string which is the default exchange.",
    )
    parser.add_argument(
        "--service-name",
        metavar="<pattern>",
        type=str,
        default="clock-service",
        help="The service name. Defaults to 'clock-service'.",
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

    async def on_request(payload: Any, message: IncomingMessage):
        use_utc = payload["utc"]
        sequence_number = payload["sequence_number"]
        origin = message.headers["From"]
        logger.info(f"Received request from '{origin}': {payload}")
        now = datetime.datetime.now(tz=datetime.timezone.utc if use_utc else None)
        response = dict(
            sequence_number=sequence_number,
            time=now.time().isoformat(),
            date=now.date().isoformat(),
            timestamp=now.timestamp(),
        )

        if random.random() > 0.8:
            logger.info("Simulating a processing error")
            raise Exception("Boom!")

        logger.info(f"Returning response: {response}")
        return response

    r = Responder(
        amqp_url=args.amqp_url,
        exchange_name=args.exchange_name,
        service_name=args.service_name,
        serialization=CONTENT_TYPE_JSON,
        on_request=on_request,
    )

    run(r.start, finalize=r.stop)
