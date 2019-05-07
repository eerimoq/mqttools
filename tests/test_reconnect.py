import logging
import asyncio
import unittest
import threading
import queue
import socket

import mqttools


HOST = 'localhost'
PORT = 0


class Client(threading.Thread):

    def __init__(self, host, port):
        super().__init__()
        self._host = host
        self._port = port

    async def client(self):
        client = mqttools.Client(self._host,
                                 self._port,
                                 'goo',
                                 response_timeout=1,
                                 keep_alive_s=1)

        await client.start()
        await client.subscribe('/a/b', 0)

        while True:
            await client.messages.get()

    def run(self):
        asyncio.run(self.client())


class ReconnectTest(unittest.TestCase):

    def test_reconnect(self):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind((HOST, PORT))
        listener.listen()

        client_thread = Client(*listener.getsockname())
        client_thread.daemon = True
        client_thread.start()

        client, _ = listener.accept()

        # CONNECT
        self.assertEqual(
            client.recv(18),
            b'\x10\x10\x00\x04MQTT\x05\x02\x00\x01\x00\x00\x03goo')
        # CONNACK
        self.assertEqual(client.send(b'\x20\x03\x00\x00\x00'), 5)
        # SUBSCRIBE
        self.assertEqual(
            client.recv(12),
            b'\x82\n\x00\x01\x00\x00\x04/a/b\x00')
        # SUBACK
        self.assertEqual(client.send(b'\x90\x04\x00\x01\x00\x00'), 6)
        # PINGREQ
        self.assertEqual(client.recv(2), b'\xc0\x00')
        # PINGRESP
        self.assertEqual(client.send(b'\xd0\x00'), 2)

        # Connection closed by the broker. Wait for the client to
        # reconnect.
        client.close()
        client, _ = listener.accept()

        # CONNECT
        self.assertEqual(
            client.recv(18),
            b'\x10\x10\x00\x04MQTT\x05\x02\x00\x01\x00\x00\x03goo')
        # CONNACK
        self.assertEqual(client.send(b'\x20\x03\x00\x00\x00'), 5)
        # SUBSCRIBE
        self.assertEqual(
            client.recv(12),
            b'\x82\n\x00\x02\x00\x00\x04/a/b\x00')
        # SUBACK
        self.assertEqual(client.send(b'\x90\x04\x00\x02\x00\x00'), 6)
        # PINGREQ
        self.assertIn(client.recv(2), [b'', b'\xc0\x00'])

        # Don't respond to the ping request. The client should
        # reconnect.
        client2, _ = listener.accept()
        client.close()

        # CONNECT
        self.assertEqual(
            client2.recv(18),
            b'\x10\x10\x00\x04MQTT\x05\x02\x00\x01\x00\x00\x03goo')
        # CONNACK
        self.assertEqual(client2.send(b'\x20\x03\x00\x00\x00'), 5)
        # SUBSCRIBE
        self.assertEqual(
            client2.recv(12),
            b'\x82\n\x00\x03\x00\x00\x04/a/b\x00')
        # SUBACK
        self.assertEqual(client2.send(b'\x90\x04\x00\x03\x00\x00'), 6)

        client2.close()
        listener.close()


logging.basicConfig(level=logging.DEBUG)


if __name__ == '__main__':
    unittest.main()
