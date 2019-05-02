|buildstatus|_
|coverage|_

MQTT Tools
==========

Requires Python 3.7 or later!

NOTE: This project is far from complete. Lots of MQTT features are not
implemented yet, and those implemented are probably buggy.

Features:

- ``asyncio`` MQTT 5.0 client (under development).

MQTT 5.0 specification:
https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html

Examples
========

Command line
------------

Subscribe
^^^^^^^^^

An example connecting to an MQTT broker, subscribing to the topic
``/test/#``, and printing all published messaged.

.. code-block:: text

   $ mqttools subscribe /test/#
   Topic:   /test
   Message: b'11'

   Topic:   /test/mqttools/foo
   Message: b'bar'

Publish
^^^^^^^

An example connecting to an MQTT broker and publishing the message
``bar`` to the topic ``/test/mqttools/foo``.

.. code-block:: text

   $ mqttools publish /test/mqttools/foo bar
   Topic:   /test/mqttools/foo
   Message: b'bar'
   QoS:     0

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
       client = mqttools.Client('test.mosquitto.org', 1883, 'mqttools-subscribe')

       await client.start()
       await client.subscribe('/test/#', 0)

       while True:
           topic, message = await client.messages.get()

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
       client = mqttools.Client('test.mosquitto.org', 1883, 'mqttools-publish')

       await client.start()
       await client.publish('/test/mqttools/foo', b'bar', 0)
       await client.stop()

   asyncio.run(publisher())

.. |buildstatus| image:: https://travis-ci.org/eerimoq/mqttools.svg?branch=master
.. _buildstatus: https://travis-ci.org/eerimoq/mqttools

.. |coverage| image:: https://coveralls.io/repos/github/eerimoq/mqttools/badge.svg?branch=master
.. _coverage: https://coveralls.io/github/eerimoq/mqttools
