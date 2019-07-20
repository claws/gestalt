try:
    import aio_pika
    from . import utils
    from . import consumer
    from . import producer
except ImportError:
    # AMQP functionality is considered optional
    pass
