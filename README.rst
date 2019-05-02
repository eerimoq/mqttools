|buildstatus|_
|coverage|_

MQTT Tools
==========

Requires Python 3.7 or later!

Features:

- ``asyncio`` MQTT client.

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
   Topic:   b'/test'
   Message: b'11'

   Topic:   b'/test/mqttools/foo'
   Message: b'bar'

Publish
^^^^^^^

An example connecting to an MQTT broker and publishing the message
``bar`` to the topic ``/test/mqttools/foo``.

.. code-block:: text

   $ mqttools publish /test/mqttools/foo bar
   Topic:   b'/test/mqttools/foo'
   Message: b'bar'
   QoS:     0

Scripting
---------

Subscribe
^^^^^^^^^

An example connecting to an MQTT broker, subscribing to the topic
``/test/#``, and printing all published messaged.

.. code-block:: python

   async def subscriber():
       client = Client('test.mosquitto.org', 1883, b'mqttools-subscribe')

       await client.start()
       await client.subscribe(b'/test/#', 0)

       while True:
           topic, message = await client.messages.get()

           print(f'Topic:   {topic}')
           print(f'Message: {message}')
           print()

   asyncio.run(subscriber())

Publish
^^^^^^^

An example connecting to an MQTT broker and publishing the message
``bar`` to the topic ``/test/mqttools/foo``.

.. code-block:: python

   async def publisher():
       client = Client('test.mosquitto.org', 1883, b'mqttools-publish')

       await client.start()
       await client.publish(b'/test/mqttools/foo', b'bar', 0)
       await client.stop()

   asyncio.run(publisher())

.. |buildstatus| image:: https://travis-ci.org/eerimoq/mqttools.svg?branch=master
.. _buildstatus: https://travis-ci.org/eerimoq/mqttools

.. |coverage| image:: https://coveralls.io/repos/github/eerimoq/mqttools/badge.svg?branch=master
.. _coverage: https://coveralls.io/github/eerimoq/mqttools
