The example in this directory uses the AMQP producer and consumer objects
to demonstrate how they can be used to transfer messages in a variety of
different formats.

The examples may be started in any order. Command line help may be obtained
by passing the ``-h`` option to the script.

In one terminal start the consumer.

```console
(venv) $ python topic-consumer.py --log-level info
```

In another terminal open the producer which will send messages, in a variety
of serialization and compression strategies, to the AMQP topic every second.

```console
(venv) $ python topic-producer.py --log-level info
```

Upon receipt of a message the consumer will print it to stdout.
