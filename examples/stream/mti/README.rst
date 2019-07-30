The example in this directory uses the message type identifier (MTI)
stream endpoint to build a client and a server application. The content
type being transferred is agreed up front, in this case it is Protobuf.

The examples may be started in any order. Command line help may be obtained
by passing the ``-h`` option to the script.

When the client connects to the server it sends a message to the server
containing a timestamp and a counter value. The server waits briefly to
simulate work (and to avoid a tight communications feedback loop!) then
replies to the client by sending the same message structure back but with
the counter value incremented. Upon receipt of this messages the client
waits briefly and sends a reply back to the server following the same
sequence.


In one terminal start the receiver.

.. code-block:: console

    (venv) $ python receiver.py --log-level debug

In another terminal open the sender which will send a message to the remote
address every second.

.. code-block:: console

    (venv) $ python sender.py --log-level debug

Upon receipt of a message the receiver will print it to stdout.
