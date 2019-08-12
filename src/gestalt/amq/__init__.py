""" Optional AMQP functionality """
from . import utils

try:
    import aio_pika
    from . import consumer
    from . import producer
    from . import requester
    from . import responder
except ImportError:
    # AMQP functionality is considered optional
    pass  # noqa
