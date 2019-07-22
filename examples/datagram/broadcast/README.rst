The example in this directory uses the base Datagram endpoint to build a
sender and a receiver application. The receiver binds locally to all
interfaces. The sender makes its best guess as to the network broadcast
address and sends messages to that every second.

The endpoint in each file is created with its content type set to JSON
which allows a Python object to be passed into the send function to be
automatically JSON encoded when sent and automatically JSON decoded upon
receipt.

In one terminal start the receiver.

.. code-block:: console

    (venv) $ python receiver.py --log-level debug

In another terminal open the sender which will send a message to the remote
address every second.

.. code-block:: console

    (venv) $ python sender.py --log-level debug

Upon receipt of a message the receiver will print it to stdout.
