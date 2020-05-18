import unittest

from mqttools.common import pack_connect
from mqttools.common import pack_u8
from mqttools.common import pack_u16
from mqttools.common import pack_u32
from mqttools.common import pack_binary
from mqttools.common import pack_string
from mqttools.common import pack_variable_integer
from mqttools.common import unpack_properties
from mqttools.common import PropertyIds
from mqttools.common import MalformedPacketError
from mqttools.common import PayloadReader


class PackUnpackTest(unittest.TestCase):

    def test_pack_connect(self):
        datas = [
            (
                ('client-id', True, '', b'', None, 0, 0, {}),
                b'\x10\x16\x00\x04MQTT\x05\x02\x00\x00\x00\x00\tclient-id'
            ),
            (
                ('abc', True, 'foo', b'bar', False, 0, 600, {}),
                b'\x10\x1b\x00\x04MQTT\x05\x06\x02X\x00\x00\x03abc\x00\x00\x03'
                b'foo\x00\x03bar'
            ),
            (
                ('abc', True, 'foo2', b'bar', True, 1, 3600, {}),
                b'\x10\x1c\x00\x04MQTT\x05\x2e\x0e\x10\x00\x00\x03abc\x00\x00'
                b'\x04foo2\x00\x03bar'
            )
        ]

        for args, expected_packed in datas:
            packed = pack_connect(*args)
            self.assertEqual(packed, expected_packed)

    def test_unpack_properties(self):
        buf = pack_u8(PropertyIds.PAYLOAD_FORMAT_INDICATOR)
        buf += pack_u8(5)
        buf += pack_u8(PropertyIds.MESSAGE_EXPIRY_INTERVAL)
        buf += pack_u32(6)
        buf += pack_u8(PropertyIds.CONTENT_TYPE)
        buf += pack_string('a')
        buf += pack_u8(PropertyIds.RESPONSE_TOPIC)
        buf += pack_string('b')
        buf += pack_u8(PropertyIds.CORRELATION_DATA)
        buf += pack_binary(b'c')
        buf += pack_u8(PropertyIds.SUBSCRIPTION_IDENTIFIER)
        buf += pack_variable_integer(7)
        buf += pack_u8(PropertyIds.SESSION_EXPIRY_INTERVAL)
        buf += pack_u32(8)
        buf += pack_u8(PropertyIds.ASSIGNED_CLIENT_IDENTIFIER)
        buf += pack_string('d')
        buf += pack_u8(PropertyIds.SERVER_KEEP_ALIVE)
        buf += pack_u16(9)
        buf += pack_u8(PropertyIds.AUTHENTICATION_METHOD)
        buf += pack_string('e')
        buf += pack_u8(PropertyIds.AUTHENTICATION_DATA)
        buf += pack_binary(b'f')
        buf += pack_u8(PropertyIds.REQUEST_PROBLEM_INFORMATION)
        buf += pack_u8(10)
        buf += pack_u8(PropertyIds.WILL_DELAY_INTERVAL)
        buf += pack_u32(11)
        buf += pack_u8(PropertyIds.REQUEST_RESPONSE_INFORMATION)
        buf += pack_u8(12)
        buf += pack_u8(PropertyIds.RESPONSE_INFORMATION)
        buf += pack_string('g')
        buf += pack_u8(PropertyIds.SERVER_REFERENCE)
        buf += pack_string('h')
        buf += pack_u8(PropertyIds.REASON_STRING)
        buf += pack_string('i')
        buf += pack_u8(PropertyIds.RECEIVE_MAXIMUM)
        buf += pack_u16(13)
        buf += pack_u8(PropertyIds.TOPIC_ALIAS_MAXIMUM)
        buf += pack_u16(14)
        buf += pack_u8(PropertyIds.TOPIC_ALIAS)
        buf += pack_u16(15)
        buf += pack_u8(PropertyIds.MAXIMUM_QOS)
        buf += pack_u8(16)
        buf += pack_u8(PropertyIds.RETAIN_AVAILABLE)
        buf += pack_u8(17)
        buf += pack_u8(PropertyIds.USER_PROPERTY)
        buf += pack_string('j')
        buf += pack_u8(PropertyIds.MAXIMUM_PACKET_SIZE)
        buf += pack_u32(18)
        buf += pack_u8(PropertyIds.WILDCARD_SUBSCRIPTION_AVAILABLE)
        buf += pack_u8(19)
        buf += pack_u8(PropertyIds.SUBSCRIPTION_IDENTIFIER_AVAILABLE)
        buf += pack_u8(20)
        buf += pack_u8(PropertyIds.SHARED_SUBSCRIPTION_AVAILABLE)
        buf += pack_u8(21)
        buf = pack_variable_integer(len(buf)) + buf

        properties = unpack_properties(
            'FOO',
            [
                PropertyIds.PAYLOAD_FORMAT_INDICATOR,
                PropertyIds.MESSAGE_EXPIRY_INTERVAL,
                PropertyIds.CONTENT_TYPE,
                PropertyIds.RESPONSE_TOPIC,
                PropertyIds.CORRELATION_DATA,
                PropertyIds.SUBSCRIPTION_IDENTIFIER,
                PropertyIds.SESSION_EXPIRY_INTERVAL,
                PropertyIds.ASSIGNED_CLIENT_IDENTIFIER,
                PropertyIds.SERVER_KEEP_ALIVE,
                PropertyIds.AUTHENTICATION_METHOD,
                PropertyIds.AUTHENTICATION_DATA,
                PropertyIds.REQUEST_PROBLEM_INFORMATION,
                PropertyIds.WILL_DELAY_INTERVAL,
                PropertyIds.REQUEST_RESPONSE_INFORMATION,
                PropertyIds.RESPONSE_INFORMATION,
                PropertyIds.SERVER_REFERENCE,
                PropertyIds.REASON_STRING,
                PropertyIds.RECEIVE_MAXIMUM,
                PropertyIds.TOPIC_ALIAS_MAXIMUM,
                PropertyIds.TOPIC_ALIAS,
                PropertyIds.MAXIMUM_QOS,
                PropertyIds.RETAIN_AVAILABLE,
                PropertyIds.USER_PROPERTY,
                PropertyIds.MAXIMUM_PACKET_SIZE,
                PropertyIds.WILDCARD_SUBSCRIPTION_AVAILABLE,
                PropertyIds.SUBSCRIPTION_IDENTIFIER_AVAILABLE,
                PropertyIds.SHARED_SUBSCRIPTION_AVAILABLE
            ],
            PayloadReader(buf))

        self.assertEqual(
            properties,
            {
                PropertyIds.PAYLOAD_FORMAT_INDICATOR: 5,
                PropertyIds.MESSAGE_EXPIRY_INTERVAL: 6,
                PropertyIds.CONTENT_TYPE: 'a',
                PropertyIds.RESPONSE_TOPIC: 'b',
                PropertyIds.CORRELATION_DATA: b'c',
                PropertyIds.SUBSCRIPTION_IDENTIFIER: 7,
                PropertyIds.SESSION_EXPIRY_INTERVAL: 8,
                PropertyIds.ASSIGNED_CLIENT_IDENTIFIER: 'd',
                PropertyIds.SERVER_KEEP_ALIVE: 9,
                PropertyIds.AUTHENTICATION_METHOD: 'e',
                PropertyIds.AUTHENTICATION_DATA: b'f',
                PropertyIds.REQUEST_PROBLEM_INFORMATION: 10,
                PropertyIds.WILL_DELAY_INTERVAL: 11,
                PropertyIds.REQUEST_RESPONSE_INFORMATION: 12,
                PropertyIds.RESPONSE_INFORMATION: 'g',
                PropertyIds.SERVER_REFERENCE: 'h',
                PropertyIds.REASON_STRING: 'i',
                PropertyIds.RECEIVE_MAXIMUM: 13,
                PropertyIds.TOPIC_ALIAS_MAXIMUM: 14,
                PropertyIds.TOPIC_ALIAS: 15,
                PropertyIds.MAXIMUM_QOS: 16,
                PropertyIds.RETAIN_AVAILABLE: 17,
                PropertyIds.USER_PROPERTY: 'j',
                PropertyIds.MAXIMUM_PACKET_SIZE: 18,
                PropertyIds.WILDCARD_SUBSCRIPTION_AVAILABLE: 19,
                PropertyIds.SUBSCRIPTION_IDENTIFIER_AVAILABLE: 20,
                PropertyIds.SHARED_SUBSCRIPTION_AVAILABLE: 21
            })

    def test_unpack_empty_properties(self):
        with self.assertRaises(MalformedPacketError):
            unpack_properties('FOO',
                              [
                                  PropertyIds.PAYLOAD_FORMAT_INDICATOR
                              ],
                              PayloadReader(b''))

    def test_unpack_invalid_property(self):
        with self.assertRaises(MalformedPacketError):
            unpack_properties('FOO',
                              [
                                  PropertyIds.PAYLOAD_FORMAT_INDICATOR
                              ],
                              PayloadReader(b'\x01\xff'))

    def test_unpack_invalid_property_for_packet(self):
        with self.assertRaises(MalformedPacketError):
            unpack_properties('FOO',
                              [
                                  PropertyIds.PAYLOAD_FORMAT_INDICATOR
                              ],
                              PayloadReader(b'\x01\x02\x00\x00\x00\x00'))


if __name__ == '__main__':
    unittest.main()
