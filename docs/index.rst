.. mqttools documentation master file, created by
   sphinx-quickstart on Sat Apr 25 11:54:09 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. toctree::
   :maxdepth: 2

.. include:: ../README.rst

Functions and classes
=====================

.. autoclass:: mqttools.Client
   :members:

.. autoclass:: mqttools.Message
   :members:

.. autoclass:: mqttools.Broker
   :members: serve_forever

.. autoclass:: mqttools.BrokerThread
   :members: stop

   .. method:: start()

      Start the broker in a thread. This function returns immediately.

.. autoclass:: mqttools.ConnectError
   :members:

.. autoclass:: mqttools.SessionResumeError
   :members:

.. autoclass:: mqttools.SubscribeError
   :members:

.. autoclass:: mqttools.UnsubscribeError
   :members:

.. autoclass:: mqttools.TimeoutError
   :members:
