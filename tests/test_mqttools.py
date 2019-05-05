import sys
import logging
import asyncio
import unittest
import threading
import binascii
import queue
import socket
from unittest.mock import patch
from io import StringIO

import mqttools


HOST = 'localhost'
PORT = 0


class Broker(threading.Thread):

    EXPECTED_DATA_INDEX = 0
    EXPECTED_DATA_STREAM = []
    ACTUAL_DATA_STREAM = []

    def __init__(self):
        super().__init__()
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.bind((HOST, PORT))
        self._listener.listen()
        self._client_closed = queue.Queue()

    @property
    def address(self):
        return self._listener.getsockname()

    def wait_for_client_closed(self):
        self._client_closed.get(timeout=1)

    def run(self):
        while True:
            print('Broker: Listening for client...')
            self.serve_client(self._listener.accept()[0])
            self._client_closed.put(True)

    def serve_client(self, client):
        print('Broker: Serving client...')

        while self.EXPECTED_DATA_INDEX < len(self.EXPECTED_DATA_STREAM):
            _, data = self.EXPECTED_DATA_STREAM[self.EXPECTED_DATA_INDEX]
            self.EXPECTED_DATA_INDEX += 1

            size = len(data)
            data = client.recv(size)
            # print(f'Broker: Received: {data}')
            self.ACTUAL_DATA_STREAM.append(('c2s', data))

            while self.EXPECTED_DATA_INDEX < len(self.EXPECTED_DATA_STREAM):
                direction, data = self.EXPECTED_DATA_STREAM[self.EXPECTED_DATA_INDEX]

                if direction != 's2c':
                    break

                self.EXPECTED_DATA_INDEX += 1
                # print(f'Broker: Sending: {data}')
                client.send(data)
                self.ACTUAL_DATA_STREAM.append(('s2c', data))

        client.close()


class MQTToolsTest(unittest.TestCase):

    def setUp(self):
        Broker.EXPECTED_DATA_INDEX = 0
        Broker.EXPECTED_DATA_STREAM = []
        Broker.ACTUAL_DATA_STREAM = []
        Broker.CLOSE_AFTER_INDEX = -1
        self.broker = Broker()
        self.broker.daemon = True
        self.broker.start()
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.broker.wait_for_client_closed()
        self.loop.close()
        self.assertEqual(Broker.ACTUAL_DATA_STREAM, Broker.EXPECTED_DATA_STREAM)

    def run_until_complete(self, coro):
        return self.loop.run_until_complete(coro)

    def test_start_stop(self):
        Broker.EXPECTED_DATA_STREAM = [
            # CONNECT
            ('c2s', b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03bar'),
            # CONNACK
            ('s2c', b'\x20\x03\x00\x00\x00'),
            # DISCONNECT
            ('c2s', b'\xe0\x02\x00\x00')
        ]

        client = mqttools.Client(*self.broker.address, 'bar')
        self.run_until_complete(client.start())
        self.run_until_complete(client.stop())

    def test_subscribe(self):
        Broker.EXPECTED_DATA_STREAM = [
            # CONNECT
            ('c2s', b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03bar'),
            # CONNACK
            ('s2c', b'\x20\x03\x00\x00\x00'),
            # SUBSCRIBE
            ('c2s', b'\x82\n\x00\x01\x00\x00\x04/a/b\x00'),
            # SUBACK
            ('s2c', b'\x90\x04\x00\x01\x00\x00'),
            # SUBSCRIBE
            ('c2s', b'\x82\n\x00\x02\x00\x00\x04/a/c\x00'),
            # SUBACK
            ('s2c', b'\x90\x04\x00\x02\x00\x00'),
            # PUBLISH
            ('s2c', b'\x30\x0a\x00\x04/a/b\x00apa'),
            # DISCONNECT
            ('c2s', b'\xe0\x02\x00\x00')
        ]

        client = mqttools.Client(*self.broker.address, 'bar')
        self.run_until_complete(client.start())
        self.run_until_complete(client.subscribe('/a/b', 0))
        self.run_until_complete(client.subscribe('/a/c', 0))
        topic, message = self.run_until_complete(client.messages.get())
        self.assertEqual(topic, '/a/b')
        self.assertEqual(message, b'apa')
        self.run_until_complete(client.stop())

    def test_publish_qos_0(self):
        Broker.EXPECTED_DATA_STREAM = [
            # CONNECT
            ('c2s', b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03bar'),
            # CONNACK
            ('s2c', b'\x20\x03\x00\x00\x00'),
            # PUBLISH
            ('c2s', b'\x30\x0a\x00\x04/a/b\x00apa'),
            # DISCONNECT
            ('c2s', b'\xe0\x02\x00\x00')
        ]

        client = mqttools.Client(*self.broker.address, 'bar')
        self.run_until_complete(client.start())
        self.run_until_complete(client.publish('/a/b', b'apa', 0))
        self.run_until_complete(client.stop())

    def test_publish_qos_1(self):
        Broker.EXPECTED_DATA_STREAM = [
            # CONNECT
            ('c2s', b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03bar'),
            # CONNACK
            ('s2c', b'\x20\x03\x00\x00\x00'),
            # PUBLISH
            ('c2s', b'\x32\x0c\x00\x04/a/b\x00\x01\x00apa'),
            # PUBACK
            ('s2c', b'\x40\x02\x00\x01'),
            # DISCONNECT
            ('c2s', b'\xe0\x02\x00\x00')
        ]

        client = mqttools.Client(*self.broker.address, 'bar')
        self.run_until_complete(client.start())
        self.run_until_complete(client.publish('/a/b', b'apa', 1))
        self.run_until_complete(client.stop())

    def test_publish_qos_2(self):
        Broker.EXPECTED_DATA_STREAM = [
            # CONNECT
            ('c2s', b'\x10\x10\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x03bar'),
            # CONNACK
            ('s2c', b'\x20\x03\x00\x00\x00'),
            # PUBLISH
            ('c2s', b'\x34\x0c\x00\x04/a/b\x00\x01\x00apa'),
            # PUBREC
            ('s2c', b'\x50\x02\x00\x01'),
            # PUBREL
            ('c2s', b'\x62\x03\x00\x01\x00'),
            # PUBCOMP
            ('s2c', b'\x70\x02\x00\x01'),
            # DISCONNECT
            ('c2s', b'\xe0\x02\x00\x00')
        ]

        client = mqttools.Client(*self.broker.address, 'bar')
        self.run_until_complete(client.start())
        self.run_until_complete(client.publish('/a/b', b'apa', 2))
        self.run_until_complete(client.stop())

    def test_command_line_publish_qos_0(self):
        Broker.EXPECTED_DATA_STREAM = [
            # CONNECT
            ('c2s', b'\x10\x1d\x00\x04MQTT\x05\x02\x00\x00\x00\x00\x10mqttools_publish'),
            # CONNACK
            ('s2c', b'\x20\x03\x00\x00\x00'),
            # PUBLISH
            ('c2s', b'\x30\x0a\x00\x04/a/b\x00apa'),
            # DISCONNECT
            ('c2s', b'\xe0\x02\x00\x00')
        ]

        argv = [
            'mqttools',
            'publish',
            '--host', self.broker.address[0],
            '--port', str(self.broker.address[1]),
            '--client-id', 'mqttools_publish',
            '/a/b',
            'apa'
        ]

        stdout = StringIO()

        with patch('sys.stdout', stdout):
            with patch('sys.argv', argv):
                mqttools.main()

        self.assertIn("Topic:   /a/b\n"
                      "Message: apa\n"
                      "QoS:     0\n",
                      stdout.getvalue())


logging.basicConfig(level=logging.DEBUG)


if __name__ == '__main__':
    unittest.main()
