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
        self.messages = []
        self.done = threading.Event()

    async def client(self):
        client = mqttools.Client(self._host,
                                 self._port,
                                 'goo',
                                 response_timeout=1,
                                 keep_alive_s=1)

        for _ in range(6):
            await client.start()
            self.messages.append(await client.messages.get())
            await client.stop()

    def run(self):
        asyncio.run(self.client())
        self.done.set()


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
        # PINGREQ
        self.assertEqual(client.recv(2), b'\xc0\x00')
        # PINGRESP
        self.assertEqual(client.send(b'\xd0\x00'), 2)

        # Connection closed by the broker.
        client.close()

        # Wait for another connection.
        client, _ = listener.accept()

        # CONNECT
        self.assertEqual(
            client.recv(18),
            b'\x10\x10\x00\x04MQTT\x05\x02\x00\x01\x00\x00\x03goo')
        # CONNACK
        self.assertEqual(client.send(b'\x20\x03\x00\x00\x00'), 5)
        # PINGREQ
        self.assertIn(client.recv(2), [b'', b'\xc0\x00'])

        # Don't respond to the ping request and wait for another
        # connection.
        client2, _ = listener.accept()
        client.close()

        # CONNECT
        self.assertEqual(
            client2.recv(18),
            b'\x10\x10\x00\x04MQTT\x05\x02\x00\x01\x00\x00\x03goo')
        # CONNACK
        self.assertEqual(client2.send(b'\x20\x03\x00\x00\x00'), 5)

        client2.close()

        # Wait for another connection.
        client, _ = listener.accept()

        # CONNECT
        self.assertEqual(
            client.recv(18),
            b'\x10\x10\x00\x04MQTT\x05\x02\x00\x01\x00\x00\x03goo')
        # CONNACK
        self.assertEqual(client.send(b'\x20\x03\x00\x00\x00'), 5)
        # DISCONNECT
        self.assertEqual(
            client.send(b'\xe0\x0f\x80\x0d\x1f\x00\x0aSome error'),
            17)

        # Wait for another connection.
        client2, _ = listener.accept()
        client.close()

        # CONNECT
        self.assertEqual(
            client2.recv(18),
            b'\x10\x10\x00\x04MQTT\x05\x02\x00\x01\x00\x00\x03goo')
        # CONNACK
        self.assertEqual(client2.send(b'\x20\x03\x00\x00\x00'), 5)
        # PUBLISH malformed packet with non-UTF-8 topic.
        self.assertEqual(client2.send(b'\x30\x07\x00\x01\xff\x00apa'), 9)
        # DISCONNECT with reason malformed packet
        self.assertEqual(client2.recv(4), b'\xe0\x02\x81\x00')

        # Wait for another connection.
        client, _ = listener.accept()
        client2.close()

        # CONNECT
        self.assertEqual(
            client.recv(18),
            b'\x10\x10\x00\x04MQTT\x05\x02\x00\x01\x00\x00\x03goo')
        # CONNACK
        self.assertEqual(client.send(b'\x20\x03\x00\x00\x00'), 5)

        client.close()
        listener.close()
        client_thread.done.wait()
        self.assertEqual(client_thread.messages, 6 * [(None, None)])


logging.basicConfig(level=logging.DEBUG)


if __name__ == '__main__':
    unittest.main()
