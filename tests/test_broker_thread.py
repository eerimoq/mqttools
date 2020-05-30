import logging
import asyncio
import unittest
import socket

import mqttools


BROKER_PORT = 10341


async def start_stop_clients():
    subscriber = mqttools.Client('localhost', BROKER_PORT)
    publisher = mqttools.Client('localhost', BROKER_PORT)

    await subscriber.start()
    await subscriber.subscribe('/apa')

    await publisher.start()
    publisher.publish('/apa', b'halloj')
    await publisher.stop()

    message = await subscriber.messages.get()

    if message != ('/apa', b'halloj'):
        raise Exception('Wrong message {}'.format(message))

    await subscriber.stop()


class BrokerThreadTest(unittest.TestCase):

    def connect(self):
        sock = socket.socket()

        try:
            sock.connect(('localhost', BROKER_PORT))
        finally:
            sock.close()

    def test_start_stop(self):
        broker = mqttools.BrokerThread(('localhost', BROKER_PORT))

        with self.assertRaises(ConnectionRefusedError):
            self.connect()

        broker.start()
        asyncio.run(start_stop_clients())
        self.connect()
        broker.stop()

        with self.assertRaises(mqttools.broker.NotRunningError):
            broker.stop()

        with self.assertRaises(ConnectionRefusedError):
            self.connect()
