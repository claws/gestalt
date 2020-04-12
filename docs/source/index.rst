.. image:: https://github.com/claws/gestalt/workflows/Build/badge.svg?branch=master
    :target: https://github.com/claws/gestalt/actions?query=branch%3Amaster

.. image:: https://img.shields.io/pypi/pyversions/gestalt
    :target: https://pypi.python.org/pypi/gestalt

.. image:: https://img.shields.io/github/license/claws/gestalt
    :target: https://github.com/claws/gestalt/blob/master/License

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
  :target: https://github.com/ambv/black


Gestalt
#######

gestalt is a Python application framework for building distributed systems.

.. toctree::
   :maxdepth: 2
   :hidden:

   user/index
   api/index
   dev/index


Quick Start
===========

Gestalt is available on PyPI and can be installed with `pip <https://pip.pypa.io>`_.

.. code-block:: console

    $ pip install gestalt

Gestalt cna use optional third party Python packages to obtain support for
features such as serialization, compression and message queuing. To install
optional extras simply specify one or more of them using the extras specifier
notation as shown in the example below which installs all of the available
extras:

.. code-block:: console

    $ pip install gestalt[amq,protobuf,msgpack,avro,brotli,snappy,yaml]

where the available extras are:

  - ``amq`` will install the AMQP extras (asyncio bindings for RabbitMQ)
  - ``protobuf`` will install support for serializing Google Protocol Buffers structures.
  - ``msgpack`` will install support for serializing Msgpack structures.
  - ``yaml`` will install support for serializing YAML structures.
  - ``avro`` will install support for serializing Apache Avro structures.
  - ``brotli`` will install Brotli compression support.
  - ``snappy`` will install Snappy compression support. The Python snappy package
    is simply a binding to a system library. Therefore you must install that first
    before the Python binding will install successfully. For example, on Debian
    systems you will want ``sudo apt-get install libsnappy-dev``

After installing Gestalt you can use it like any other Python package. Simply
import the module and use it such as is shown in the simple timer example below.

.. code-block:: python

    from gestalt.timer import Timer
    from gestalt.runner import run

    async def my_func(**kwargs):
        print("Running timer function")

    if __name__ == "__main__":

        t = Timer(1.0, my_func, forever=True)
        run(t.start(), finalize=t.stop)

Look in the examples directory for many more examples.
