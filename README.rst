.. image:: https://travis-ci.org/claws/gestalt.svg?branch=master
    :target: https://travis-ci.org/claws/gestalt

.. image:: https://img.shields.io/pypi/v/gestalt.svg
    :target: https://pypi.python.org/pypi/gestalt

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg


Gestalt
#######

Gestalt is a Python application framework for building distributed systems.
Gestalt is written in Python to run on top of asyncio.

Gestalt provides a variety of components (e.g. timers) and communications
primitives such as sockets (e.g. TCP) and message queues (i.e. AMQP).

The Advanced Message Queuing Protocol (AMQP) is an open standard protocol
specification for message passing, queuing, routing, reliability and security.
One of the most popular implementations of AMQP is RabbitMQ.

The Gestalt package builds upon the AMQP binding to add support for automatic
message serialization and compression of message payloads.

.. note::

    This project is in the early stages of development. Expects lots of
    changes.


Quickstart
==========

Gestalt is available on PyPI and can be installed using `pip <https://pip.pypa.io>`_.

The core can be installed using the command:

.. code-block:: console

    $ pip install gestalt

The optional extras can be installed using the extras specifier notation as
shown in the example bellow:

.. code-block:: console

    $ pip install gestalt[amq,protobuf]

where the available options are:

  - ``develop`` will install the development extras.
  - ``protobuf`` will install Google Protocol Buffers extras to support
    compiling proto files and serializing protobuf structures.
  - ``msgpack`` will install Msgpack extras to support serializing Msgpack
    structures.
  - ``yaml`` will install YAML extras to support serializing YAML structures.
  - ``avro`` will install Apache Avro extras to support serializing Avro
    structures.
  - ``snappy`` will install Snappy compression support.
  - ``brotli`` will install Brotli compression support.
  - ``amq`` will install the AMQP extras (asyncio bindings for RabbitMQ)

Once installed you can begin developing an application using Gestalt.

The `API Reference <http://gestalt.readthedocs.io>`_ provides API-level documentation.


Performance Tip
===============

Use the high performance `uvloop <https://github.com/MagicStack/uvloop>`_ event loop
implementation instead of the default asyncio event loop.

Install the package using:

.. code-block:: console

    (venv) $ pip install uvloop

Then, simply call ``uvloop.install()`` manually before creating an asyncio event
loop either via the standard ``asyncio.run()`` or the ``gestalt.runner.run``
function.
