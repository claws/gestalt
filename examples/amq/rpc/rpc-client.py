import argparse
import asyncio
import datetime
import logging
import random
import time
from asyncio import CancelledError, TimeoutError
from aio_pika.exceptions import DeliveryError
from gestalt.amq.requester import Requester
from gestalt.serialization import CONTENT_TYPE_JSON
from gestalt.runner import run

from aio_pika import IncomingMessage
from typing import Any

logger = logging.getLogger(__name__)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="AMQP RPC Client Example")
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

    r = Requester(
        amqp_url=args.amqp_url,
        exchange_name=args.exchange_name,
        service_name=args.service_name,
        serialization=CONTENT_TYPE_JSON,
    )

    async def message_requester(r: Requester) -> None:
        """ Generate a new request message, in various formats, and publish it """

        counter = 0
        while True:
            counter += 1

            request_msg = dict(sequence_number=counter, utc=True)
            # For demonstration purposes randomly choose to use an invalid
            # service name to show that the message gets returned and raises
            # a DeliveryError exception.
            service_name = (
                r.service_name if random.random() < 0.8 else "invalid_service_name"
            )
            try:
                logger.info(f"Sending request {request_msg} to {service_name}")
                response_msg = await r.request(
                    request_msg, expiration=2, service_name=service_name
                )
                logger.info(f"Received response: {response_msg}")
            except TimeoutError as exc:
                logger.info(f"Request was timed-out: {exc}")
            except CancelledError as exc:
                logger.info(f"Request was cancelled: {exc}")
            except DeliveryError as exc:
                logger.info(f"Request delivery error: {exc}")

            # Wait some time before sending another request
            await asyncio.sleep(3)

    async def start_requesting(r):
        await r.start()
        await asyncio.sleep(1)
        asyncio.get_event_loop().create_task(message_requester(r))

    run(start_requesting(r), finalize=r.stop)
