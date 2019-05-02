import logging
import asyncio
import unittest
import threading
import binascii
import queue
import socket

import mqttools


class MQTToolsPackUnpackTest(unittest.TestCase):

    def test_pack_connect(self):
        datas = [
            (
                (
                    b'client-id',
                    b'',
                    b'',
                    0,
                    0
                ),
                b'\x10\x16\x00\x04MQTT\x05\x02\x00\x00\x00\x00\tclient-id'
            ),
            (
                (
                    b'abc',
                    b'foo',
                    b'bar',
                    0,
                    600
                ),
                b'\x10\x1b\x00\x04MQTT\x05\x06\x02X\x00\x00\x03abc\x00\x00\x03'
                b'foo\x00\x03bar'
            ),
            (
                (
                    b'abc',
                    b'foo2',
                    b'bar',
                    1,
                    3600
                ),
                b'\x10\x1c\x00\x04MQTT\x05\x0e\x0e\x10\x00\x00\x03abc\x00\x00'
                b'\x04foo2\x00\x03bar'
            )
        ]

        for args, expected_packed in datas:
            packed = mqttools.pack_connect(*args)
            self.assertEqual(packed, expected_packed)


if __name__ == '__main__':
    unittest.main()
