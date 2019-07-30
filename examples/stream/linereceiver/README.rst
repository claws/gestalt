The example in this directory uses a base stream endpoint to build a
client and a server application that communicate using a custom protocol
that uses the end-of-line (i.e. '\n') character to delimited separate
messages.

The custom protocol used is very basic as it assumes that end-of-line
delimiters will never exists within a message payload.

The examples may be started in any order. Command line help may be obtained
by passing the ``-h`` option to the script.

When the client connects to the server it sends a message to the server
containing a timestamp. The server waits briefly to simulate work (and to
avoid a tight communications feedback loop!) then replies to the client
by sending a similar message structure back. Upon receipt of this messages
the client waits briefly and sends a reply back to the server following the
same sequence.


In one terminal start the receiver.

.. code-block:: console

    (venv) $ python receiver.py --log-level debug

In another terminal open the sender which will send a message to the remote
address every second.

.. code-block:: console

    (venv) $ python sender.py --log-level debug

Upon receipt of a message the receiver will print it to stdout.
