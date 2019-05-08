|buildstatus|_
|coverage|_

MQTT Tools
==========

MQTT tools in Python 3.7 and later.

Features:

- MQTT 5.0 client using ``asyncio``.

- Subscribe to and publish QoS 0 messages.

- Broker session resume (or clean start support) for less initial
  communication.

- Topic aliases for smaller publish packets.

- Monitor, subscribe and publish command line commands.

QoS 1 and QoS 2 messages are not yet implemented. A client side
session state storage is required to do so.

MQTT 5.0 specification:
https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html

Project homepage: https://github.com/eerimoq/mqttools

Documentation: https://mqttools.readthedocs.org/en/latest

Installation
============

.. code-block:: python

    pip install mqttools

Examples
========

Command line
------------

Subscribe
^^^^^^^^^

Connect to given MQTT broker and subscribe to a topic. All received
messages are printed to standard output.

.. code-block:: text

   $ mqttools subscribe /test/#
   Topic:   /test
   Message: 11

   Topic:   /test/mqttools/foo
   Message: bar

Publish
^^^^^^^

Connect to given MQTT broker and publish a message to a topic.

.. code-block:: text

   $ mqttools publish /test/mqttools/foo bar
   Published 1 message(s) in 0 seconds from 1 concurrent task(s).

Publish multiple messages as quickly as possible with ``--count`` to
benchmark the client and the broker.

.. code-block:: text

   $ mqttools publish --count 100 /test/mqttools/foo
   Published 100 message(s) in 0.39 seconds from 10 concurrent task(s).

Monitor
^^^^^^^

Connect to given MQTT broker and monitor given topics in a text based
user interface.

.. code-block:: text

   $ mqttools monitor /test/#

.. image:: https://github.com/eerimoq/mqttools/raw/master/docs/monitor.png

The menu at the bottom of the monitor shows the available commands.

- Quit: Quit the monitor. Ctrl-C can be used as well.

- Play/Pause: Toggle between playing and paused (or running and freezed).

Scripting
---------

Subscribe
^^^^^^^^^

An example connecting to an MQTT broker, subscribing to the topic
``/test/#``, and printing all published messaged.

.. code-block:: python

   import asyncio
   import mqttools

   async def subscriber():
       client = mqttools.Client('broker.hivemq.com', 1883)

       await client.start()
       await client.subscribe('/test/#')

       while True:
           topic, message = await client.messages.get()

           if topic is None:
               print('Broker connection lost!')
               break

           print(f'Topic:   {topic}')
           print(f'Message: {message}')

   asyncio.run(subscriber())

Publish
^^^^^^^

An example connecting to an MQTT broker and publishing the message
``bar`` to the topic ``/test/mqttools/foo``.

.. code-block:: python

   import asyncio
   import mqttools

   async def publisher():
       client = mqttools.Client('broker.hivemq.com', 1883)

       await client.start()
       client.publish('/test/mqttools/foo', b'bar')
       await client.stop()

   asyncio.run(publisher())

.. |buildstatus| image:: https://travis-ci.org/eerimoq/mqttools.svg?branch=master
.. _buildstatus: https://travis-ci.org/eerimoq/mqttools

.. |coverage| image:: https://coveralls.io/repos/github/eerimoq/mqttools/badge.svg?branch=master
.. _coverage: https://coveralls.io/github/eerimoq/mqttools
