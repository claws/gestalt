The example in this directory uses the AMQP requester and responder objects
to demonstrate how they can be used to transfer messages using a RPC style.

The examples may be started in any order. Command line help may be obtained
by passing the ``-h`` option to the script.

In one terminal start the responder (with logging set to 'info' if you want
to see informative output).

```console
(venv) $ python rpc-server.py --log-level info
```

In another terminal open the client (with logging set to 'info' if you want
to see informative output). The client will send request messages every
second.

```console
(venv) $ python rpc-client.py --log-level info
```

Upon receipt of a request message the responder will print it to stdout and
send a response. The responder randomly generates an exception when processing
a response to simulate a response failure. In this case the client should
detect the response failure and report it.
