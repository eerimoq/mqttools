import logging
import asyncio
import unittest
import threading
import binascii
from socketserver import StreamRequestHandler
from socketserver import ThreadingMixIn
from socketserver import TCPServer

import mqttools


HOST = 'localhost'
PORT = 0


class ClientHandler(StreamRequestHandler):

    EXPECTED_DATA_INDEX = 0
    EXPECTED_DATA_STREAM = []
    ACTUAL_DATA_STREAM = []

    def handle(self):
        while self.EXPECTED_DATA_INDEX < len(self.EXPECTED_DATA_STREAM):
            _, data = self.EXPECTED_DATA_STREAM[self.EXPECTED_DATA_INDEX]
            self.EXPECTED_DATA_INDEX += 1

            size = len(data)
            # print(f'Broker: Reading {size} bytes.')
            data = self.rfile.read(size)
            # print(f'Broker: Read {binascii.hexlify(data)}')
            self.ACTUAL_DATA_STREAM.append(('c2s', data))

            while self.EXPECTED_DATA_INDEX < len(self.EXPECTED_DATA_STREAM):
                direction, data = self.EXPECTED_DATA_STREAM[self.EXPECTED_DATA_INDEX]

                if direction != 's2c':
                    break

                self.EXPECTED_DATA_INDEX += 1
                # print(f'Broker: Writing {binascii.hexlify(data)}')
                self.wfile.write(data)
                self.ACTUAL_DATA_STREAM.append(('s2c', data))


class BrokerThread(ThreadingMixIn, TCPServer):
    pass


class MQTToolsTest(unittest.TestCase):

    def setUp(self):
        ClientHandler.EXPECTED_DATA_INDEX = 0
        ClientHandler.EXPECTED_DATA_STREAM = []
        ClientHandler.ACTUAL_DATA_STREAM = []
        self.broker = BrokerThread((HOST, PORT), ClientHandler)
        self.broker_thread = threading.Thread(target=self.broker.serve_forever)
        self.broker_thread.daemon = True
        self.broker_thread.start()
        self.loop = asyncio.get_event_loop()

    def tearDown(self):
        self.broker.shutdown()
        self.broker.server_close()
        self.assertEqual(ClientHandler.ACTUAL_DATA_STREAM,
                         ClientHandler.EXPECTED_DATA_STREAM)

    def run_until_complete(self, coro):
        self.loop.run_until_complete(coro)

    def test_start_stop(self):
        ClientHandler.EXPECTED_DATA_STREAM = [
            # CONNECT
            (
                'c2s',
                b'\x10\x0f\x00\x04\x4d\x51\x54\x54\x04\x02\x00\x00\x00\x03\x62'
                b'\x61\x72'
            ),
            # CONNACK
            ('s2c', b'\x20\x02\x00\x00'),
            # DISCONNECT
            ('c2s', b'\xe0\x00')
        ]

        client = mqttools.Client(*self.broker.server_address, b'bar')
        self.run_until_complete(client.start())
        self.run_until_complete(client.stop())

    def test_subscribe(self):
        ClientHandler.EXPECTED_DATA_STREAM = [
            # CONNECT
            (
                'c2s',
                b'\x10\x0f\x00\x04\x4d\x51\x54\x54\x04\x02\x00\x00\x00\x03\x62'
                b'\x61\x72'
            ),
            # CONNACK
            ('s2c', b'\x20\x02\x00\x00'),
            # SUBSCRIBE
            ('c2s', b'\x82\x09\x00\x01\x00\x04\x2f\x61\x2f\x62\x00'),
            # SUBACK
            ('s2c', b'\x90\x03\x00\x01\x00'),
            # DISCONNECT
            ('c2s', b'\xe0\x00')
        ]

        client = mqttools.Client(*self.broker.server_address, b'bar')
        self.run_until_complete(client.start())
        self.run_until_complete(client.subscribe(b'/a/b', 0))
        self.run_until_complete(client.stop())

    def test_publish_qos_0(self):
        ClientHandler.EXPECTED_DATA_STREAM = [
            # CONNECT
            (
                'c2s',
                b'\x10\x0f\x00\x04\x4d\x51\x54\x54\x04\x02\x00\x00\x00\x03\x62'
                b'\x61\x72'
            ),
            # CONNACK
            ('s2c', b'\x20\x02\x00\x00'),
            # PUBLISH
            (
                'c2s',
                b'\x30\x0e\x00\x09\x2f\x74\x65\x73\x74\x2f\x66\x6f\x6f\x61'
                b'\x70\x61'
            ),
            # DISCONNECT
            ('c2s', b'\xe0\x00')
        ]

        client = mqttools.Client(*self.broker.server_address, b'bar')
        self.run_until_complete(client.start())
        self.run_until_complete(client.publish(b'/test/foo', b'apa', 0))
        self.run_until_complete(client.stop())


logging.basicConfig(level=logging.DEBUG)


if __name__ == '__main__':
    unittest.main()
