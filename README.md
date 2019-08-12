[![Build Status](https://travis-ci.org/claws/gestalt.svg?branch=master)](https://travis-ci.org/claws/gestalt) [![pypi](https://img.shields.io/pypi/v/gestalt.svg)](https://pypi.python.org/pypi/gestalt) [![Documentation Status](https://readthedocs.org/projects/gestalt/badge/?version=latest)](https://gestalt.readthedocs.io/en/latest) ![License](https://img.shields.io/github/license/claws/gestalt) ![pyversions](https://img.shields.io/pypi/pyversions/gestalt) ![Style](https://img.shields.io/badge/code%20style-black-000000.svg)

# Gestalt

Gestalt is an asyncio based Python package to help build distributed systems.

Gestalt provides a variety of building blocks to assist building long-running
network applications that leverage asyncio concurrency primitives.

```
gestalt:
an organized whole that is perceived as more than the sum of its parts.
```

> This project is in the early stages of development. Expects lots of changes.


## Features

The main features of Gestalt are:

- Inter-process Communications

  - Automatic serialization and compression of message payloads.
  - Socket endpoints.

    - TCP

      - Stream
      - Netstring
      - Message Type Identifier

    - UDP

      - Datagram
      - Message Type Identifier

  - Message queuing (i.e. AMQP) components. The Advanced Message
    Queuing Protocol (AMQP) is an open standard protocol specification for
    message passing, queuing, routing, reliability and security. One of the
    most popular implementations of AMQP is RabbitMQ. The Gestalt package
    provides high level components that support automatic message serialization
    and compression of message payloads.

    - Topic Publisher and Subscriber
    - Request and Reply (RPC)

- A Timer component that simplifies creating periodic calls to a function.
  Timers can be created as single-shot, repeat a specified number of times
  or repeat forever.

- An application runner that simplifies running an asyncio application by
  performing common setup such as creating an event loop, registering signal
  handlers, registering a global exception handler and performing graceful
  shutdown.

## Installation

Gestalt is available on PyPI and can be installed using [pip](https://pip.pypa.io).

Install core functionality using the command:

```console
$ pip install gestalt
```

Gestalt uses optional third party Python packages to obtain support for
features such as serialization, compression and message queuing. To install
optional extras simply specify one or more of them using the extras specifier
notation as shown in the example below which installs all of the available
extras:

```console
$ pip install gestalt[amq,protobuf,msgpack,avro,brotli,snappy,yaml]
```

where the available extras are:

- ``protobuf`` will install support for serializing Google Protocol Buffers structures.
- ``msgpack`` will install support for serializing Msgpack structures.
- ``yaml`` will install support for serializing YAML structures.
- ``avro`` will install support for serializing Apache Avro structures.
- ``snappy`` will install Snappy compression support. The Python snappy package
  is simply a binding to a system library. Therefore you must install that first
  before the Python binding will install successfully. For example, on Debian
  systems you will want ``sudo apt-get install libsnappy-dev``

- ``brotli`` will install Brotli compression support.
- ``amq`` will install the AMQP extras (asyncio bindings for RabbitMQ)
- ``develop`` will install the extras needed to development Gestalt.

Once installed you can begin using Gestalt to develop applications.

## Usage

There are many examples under the ``examples`` directory.

The [API Reference](http://gestalt.readthedocs.io) provides API-level documentation.


### Performance Tip

Consider using the high performance [uvloop](https://github.com/MagicStack/uvloop)
event loop implementation instead of the default asyncio event loop.

Install the package using:

```console
(venv) $ pip install uvloop
```

Then, simply call ``uvloop.install()`` before creating an asyncio event loop
either via the standard ``asyncio.get_event_loop()``, ``asyncio.run()`` or the
``gestalt.runner.run`` function.
