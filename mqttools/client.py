import os
import logging
import asyncio
import struct
import enum
import binascii
from io import BytesIO
import uuid
import bitstruct


# Control packet types.
class ControlPacketType(enum.IntEnum):
    CONNECT     = 1
    CONNACK     = 2
    PUBLISH     = 3
    PUBACK      = 4
    PUBREC      = 5
    PUBREL      = 6
    PUBCOMP     = 7
    SUBSCRIBE   = 8
    SUBACK      = 9
    UNSUBSCRIBE = 10
    UNSUBACK    = 11
    PINGREQ     = 12
    PINGRESP    = 13
    DISCONNECT  = 14


# Connection flags.
CLEAN_START    = 0x02
WILL_FLAG      = 0x04
WILL_QOS_1     = 0x08
WILL_QOS_2     = 0x10
WILL_RETAIN    = 0x20
PASSWORD_FLAG  = 0x40
USER_NAME_FLAG = 0x80


class ConnectReasonCode(enum.IntEnum):
    SUCCESS                              = 0
    V3_1_1_UNACCEPTABLE_PROTOCOL_VERSION = 1
    V3_1_1_IDENTIFIER_REJECTED           = 2
    V3_1_1_SERVER_UNAVAILABLE            = 3
    V3_1_1_BAD_USER_NAME_OR_PASSWORD     = 4
    V3_1_1_NOT_AUTHORIZED                = 5
    UNSPECIFIED_ERROR                    = 128
    MALFORMED_PACKET                     = 129
    PROTOCOL_ERROR                       = 130
    IMPLEMENTATION_SPECIFIC_ERROR        = 131
    UNSUPPORTED_PROTOCOL_VERSION         = 132
    CLIENT_IDENTIFIER_NOT_VALID          = 133
    BAD_USER_NAME_OR_PASSWORD            = 134
    NOT_AUTHORIZED                       = 135
    SERVER_UNAVAILABLE                   = 136
    SERVER_BUSY                          = 137
    BANNED                               = 138
    BAD_AUTHENTICATION_METHOD            = 140
    TOPIC_NAME_INVALID                   = 144
    PACKET_TOO_LARGE                     = 149
    QUOTA_EXCEEDED                       = 151
    PAYLOAD_FORMAT_INVALID               = 153
    RETAIN_NOT_SUPPORTED                 = 154
    QOS_NOT_SUPPORTED                    = 155
    USE_ANOTHER_SERVER                   = 156
    SERVER_MOVED                         = 157
    CONNECTION_RATE_EXCEEDED             = 159


class PropertyIds(enum.IntEnum):
    PAYLOAD_FORMAT_INDICATOR          = 1
    MESSAGE_EXPIRY_INTERVAL           = 2
    CONTENT_TYPE                      = 3
    RESPONSE_TOPIC                    = 8
    CORRELATION_DATA                  = 9
    SUBSCRIPTION_IDENTIFIER           = 11
    SESSION_EXPIRY_INTERVAL           = 17
    ASSIGNED_CLIENT_IDENTIFIER        = 18
    SERVER_KEEP_ALIVE                 = 19
    AUTHENTICATION_METHOD             = 21
    AUTHENTICATION_DATA               = 22
    REQUEST_PROBLEM_INFORMATION       = 23
    WILL_DELAY_INTERVAL               = 24
    REQUEST_RESPONSE_INFORMATION      = 25
    RESPONSE_INFORMATION              = 26
    SERVER_REFERENCE                  = 28
    REASON_STRING                     = 31
    RECEIVE_MAXIMUM                   = 33
    TOPIC_ALIAS_MAXIMUM               = 34
    TOPIC_ALIAS                       = 35
    MAXIMUM_QOS                       = 36
    RETAIN_AVAILABLE                  = 37
    USER_PROPERTY                     = 38
    MAXIMUM_PACKET_SIZE               = 39
    WILDCARD_SUBSCRIPTION_AVAILABLE   = 40
    SUBSCRIPTION_IDENTIFIER_AVAILABLE = 41
    SHARED_SUBSCRIPTION_AVAILABLE     = 42


class DisconnectReasonCode(enum.IntEnum):
    NORMAL_DISCONNECTION                   = 0
    DISCONNECT_WITH_WILL_MESSAGE           = 4
    UNSPECIFIED_ERROR                      = 128
    MALFORMED_PACKET                       = 129
    PROTOCOL_ERROR                         = 130
    IMPLEMENTATION_SPECIFIC_ERROR          = 131
    NOT_AUTHORIZED                         = 135
    SERVER_BUSY                            = 137
    SERVER_SHUTTING_DOWN                   = 139
    KEEP_ALIVE_TIMEOUT                     = 141
    SESSION_TAKEN_OVER                     = 142
    TOPIC_FILTER_INVALID                   = 143
    TOPIC_NAME_INVALID                     = 144
    RECEIVE_MAXIMUM_EXCEEDED               = 147
    TOPIC_ALIAS_INVALID                    = 148
    PACKET_TOO_LARGE                       = 149
    MESSAGE_RATE_TOO_HIGH                  = 150
    QUOTA_EXCEEDED                         = 151
    ADMINISTRATIVE_ACTION                  = 152
    PAYLOAD_FORMAT_INVALID                 = 153
    RETAIN_NOT_SUPPORTED                   = 154
    QOS_NOT_SUPPORTED                      = 155
    USE_ANOTHER_SERVER                     = 156
    SERVER_MOVED                           = 157
    SHARED_SUBSCRIPTIONS_NOT_SUPPORTED     = 158
    CONNECTION_RATE_EXCEEDED               = 159
    MAXIMUM_CONNECT_TIME                   = 160
    SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED = 161
    WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED   = 162


class PubackReasonCode(enum.IntEnum):
    SUCCESS                       = 0
    NO_MATCHING_SUBSCRIBERS       = 16
    UNSPECIFIED_ERROR             = 128
    IMPLEMENTATION_SPECIFIC_ERROR = 131
    NOT_AUTHORIZED                = 135
    TOPIC_NAME_INVALID            = 144
    PACKET_IDENTIFIER_IN_USE      = 145
    QUOTA_EXCEEDED                = 151
    PAYLOAD_FORMAT_INVALID        = 153


class PubrecReasonCode(enum.IntEnum):
    SUCCESS                       = 0
    NO_MATCHING_SUBSCRIBERS       = 16
    UNSPECIFIED_ERROR             = 128
    IMPLEMENTATION_SPECIFIC_ERROR = 131
    NOT_AUTHORIZED                = 135
    TOPIC_NAME_INVALID            = 144
    PACKET_IDENTIFIER_IN_USE      = 145
    QUOTA_EXCEEDED                = 151
    PAYLOAD_FORMAT_INVALID        = 153


class PubrelReasonCode(enum.IntEnum):
    SUCCESS = 0
    PACKET_IDENTIFIER_NOT_FOUND = 146


class PubcompReasonCode(enum.IntEnum):
    SUCCESS = 0
    PACKET_IDENTIFIER_NOT_FOUND = 146

# MQTT 5.0
PROTOCOL_VERSION = 5

LOGGER = logging.getLogger(__name__)


class Error(Exception):
    pass


class MalformedPacketError(Exception):
    pass


class ConnectError(Exception):

    def __init__(self, reason):
        super().__init__()
        self.reason = reason

    def __str__(self):
        message = f'{self.reason.name}({self.reason.value})'

        if self.reason == ConnectReasonCode.V3_1_1_UNACCEPTABLE_PROTOCOL_VERSION:
            message += ': The broker does not support protocol version 5.'

        return message


class PublishError(Exception):

    def __init__(self, reason):
        super().__init__()
        self.reason = reason

    def __str__(self):
        return f'{self.reason.name}({self.reason.value})'


class PayloadReader(BytesIO):

    def read(self, size):
        data = super().read(size)

        if len(data) != size:
            raise MalformedPacketError()

        return data

    def read_all(self):
        return super().read()

    def is_data_available(self):
        pos = self.tell()
        self.seek(0, os.SEEK_END)
        at_end = (pos < self.tell())
        self.seek(pos)

        return at_end


def control_packet_type_to_string(control_packet_type):
    try:
        name = ControlPacketType(control_packet_type).name
    except ValueError:
        name = 'UNKNOWN'

    return f'{name}({control_packet_type})'


def pack_string(data):
    data = data.encode('utf-8')
    packed = struct.pack('>H', len(data))
    packed += data

    return packed


def unpack_string(payload):
    size = unpack_u16(payload)

    try:
        return payload.read(size).decode('utf-8')
    except UnicodeDecodeError:
        raise MalformedPacketError('String not UTF-8 encoded.')


def pack_u32(value):
    return struct.pack('>I', value)


def unpack_u32(payload):
    return struct.unpack('>I', payload.read(4))[0]


def pack_u16(value):
    return struct.pack('>H', value)


def unpack_u16(payload):
    return struct.unpack('>H', payload.read(2))[0]


def pack_u8(value):
    return struct.pack('B', value)


def unpack_u8(payload):
    return payload.read(1)[0]


def unpack_property(property_id, payload):
    return {
        PropertyIds.PAYLOAD_FORMAT_INDICATOR: unpack_u8,
        PropertyIds.MESSAGE_EXPIRY_INTERVAL: unpack_u32,
        PropertyIds.CONTENT_TYPE: unpack_string,
        PropertyIds.RESPONSE_TOPIC: unpack_string,
        PropertyIds.CORRELATION_DATA: unpack_binary,
        PropertyIds.SUBSCRIPTION_IDENTIFIER: unpack_variable_integer,
        PropertyIds.SESSION_EXPIRY_INTERVAL: unpack_u32,
        PropertyIds.ASSIGNED_CLIENT_IDENTIFIER: unpack_string,
        PropertyIds.SERVER_KEEP_ALIVE: unpack_u16,
        PropertyIds.AUTHENTICATION_METHOD: unpack_string,
        PropertyIds.AUTHENTICATION_DATA: unpack_binary,
        PropertyIds.REQUEST_PROBLEM_INFORMATION: unpack_u8,
        PropertyIds.WILL_DELAY_INTERVAL: unpack_u32,
        PropertyIds.REQUEST_RESPONSE_INFORMATION: unpack_u8,
        PropertyIds.RESPONSE_INFORMATION: unpack_string,
        PropertyIds.SERVER_REFERENCE: unpack_string,
        PropertyIds.REASON_STRING: unpack_string,
        PropertyIds.RECEIVE_MAXIMUM: unpack_u16,
        PropertyIds.TOPIC_ALIAS_MAXIMUM: unpack_u16,
        PropertyIds.TOPIC_ALIAS: unpack_u16,
        PropertyIds.MAXIMUM_QOS: unpack_u8,
        PropertyIds.RETAIN_AVAILABLE: unpack_u8,
        PropertyIds.USER_PROPERTY: unpack_string,
        PropertyIds.MAXIMUM_PACKET_SIZE: unpack_u32,
        PropertyIds.WILDCARD_SUBSCRIPTION_AVAILABLE: unpack_u8,
        PropertyIds.SUBSCRIPTION_IDENTIFIER_AVAILABLE: unpack_u8,
        PropertyIds.SHARED_SUBSCRIPTION_AVAILABLE: unpack_u8
    }[property_id](payload)


def unpack_properties(packet_name,
                      allowed_property_ids,
                      payload):
    """Return a dictionary of unpacked properties, or None on failure.

    """

    end_pos = unpack_variable_integer(payload)
    end_pos += payload.tell()
    properties = {}

    while payload.tell() < end_pos:
        property_id = payload.read(1)[0]

        if property_id not in allowed_property_ids:
            raise MalformedPacketError(
                f'Invalid property identifier {property_id}.')

        property_id = PropertyIds(property_id)
        properties[property_id] = unpack_property(property_id, payload)

    # Log the properties.
    LOGGER.debug('%s properties:', packet_name)

    for identifier, value in properties.items():
        LOGGER.debug('  %s(%d): %s',
                     identifier.name,
                     identifier.value,
                     value)

    return properties


def pack_binary(data):
    packed = struct.pack('>H', len(data))
    packed += data

    return packed


def unpack_binary(payload):
    size = unpack_u16(payload)

    return payload.read(size)


def pack_variable_integer(value):
    if value == 0:
        packed = b'\x00'
    else:
        packed = b''

        while value > 0:
            encoded_byte = (value & 0x7f)
            value >>= 7

            if value > 0:
                encoded_byte |= 0x80

            packed += struct.pack('B', encoded_byte)

    return packed


def unpack_variable_integer(payload):
    value = 0
    multiplier = 1
    byte = 0x80

    while (byte & 0x80) == 0x80:
        byte = unpack_u8(payload)
        value += ((byte & 0x7f) * multiplier)
        multiplier <<= 7

    return value


def pack_fixed_header(message_type, flags, size):
    packed = bitstruct.pack('u4u4', message_type, flags)
    packed += pack_variable_integer(size)

    return packed


def unpack_packet_type(payload):
    packet_type = bitstruct.unpack('u4', payload)[0]

    try:
        packet_type = ControlPacketType(packet_type)
    except ValueError:
        pass

    return packet_type


def pack_connect(client_id,
                 will_topic,
                 will_message,
                 will_qos,
                 keep_alive_s):
    flags = CLEAN_START
    payload_length = len(client_id) + 2

    if will_topic and will_message:
        flags |= WILL_FLAG

        if will_qos == 1:
            flags |= WILL_QOS_1
        elif will_qos == 2:
            flags |= WILL_QOS_2

        payload_length += 1
        packed_will_topic = pack_string(will_topic)
        payload_length += len(packed_will_topic)
        payload_length += len(will_message) + 2

    packed = pack_fixed_header(ControlPacketType.CONNECT,
                               0,
                               10 + payload_length + 1)
    packed += struct.pack('>H', 4)
    packed += b'MQTT'
    packed += struct.pack('>BBHB',
                          PROTOCOL_VERSION,
                          flags,
                          keep_alive_s,
                          0)
    packed += pack_string(client_id)

    if flags & WILL_FLAG:
        packed += pack_variable_integer(0)
        packed += packed_will_topic
        packed += pack_binary(will_message)

    return packed


def unpack_connack(payload):
    flags = unpack_u8(payload)
    session_present = bool(flags & 1)
    reason = unpack_u8(payload)

    try:
        reason = ConnectReasonCode(reason)
    except ValueError:
        raise MalformedPacketError(f'Invalid CONNACK reason {reason}')

    properties = unpack_properties(
        'CONNACK',
        [
            PropertyIds.SESSION_EXPIRY_INTERVAL,
            PropertyIds.ASSIGNED_CLIENT_IDENTIFIER,
            PropertyIds.SERVER_KEEP_ALIVE,
            PropertyIds.AUTHENTICATION_METHOD,
            PropertyIds.AUTHENTICATION_DATA,
            PropertyIds.RESPONSE_INFORMATION,
            PropertyIds.SERVER_REFERENCE,
            PropertyIds.REASON_STRING,
            PropertyIds.RECEIVE_MAXIMUM,
            PropertyIds.TOPIC_ALIAS_MAXIMUM,
            PropertyIds.MAXIMUM_QOS,
            PropertyIds.RETAIN_AVAILABLE,
            PropertyIds.USER_PROPERTY,
            PropertyIds.MAXIMUM_PACKET_SIZE,
            PropertyIds.WILDCARD_SUBSCRIPTION_AVAILABLE,
            PropertyIds.SUBSCRIPTION_IDENTIFIER_AVAILABLE,
            PropertyIds.SHARED_SUBSCRIPTION_AVAILABLE
        ],
        payload)

    return session_present, reason, properties


def pack_disconnect():
    packed = pack_fixed_header(ControlPacketType.DISCONNECT, 0, 2)
    packed += struct.pack('B', DisconnectReasonCode.NORMAL_DISCONNECTION)
    packed += pack_variable_integer(0)

    return packed


def pack_subscribe(topic, qos, packet_identifier):
    packed_topic = pack_string(topic)
    packed = pack_fixed_header(ControlPacketType.SUBSCRIBE,
                               2,
                               len(packed_topic) + 4)
    packed += struct.pack('>HB', packet_identifier, 0)
    packed += packed_topic
    packed += struct.pack('B', qos)

    return packed


def unpack_suback(payload):
    packet_identifier = unpack_u16(payload)
    properties = unpack_properties('SUBACK',
                                   [
                                       PropertyIds.REASON_STRING,
                                       PropertyIds.USER_PROPERTY
                                   ],
                                   payload)

    return packet_identifier, properties


def pack_publish(topic, message, qos, packet_identifier):
    packed_topic = pack_string(topic)
    size = len(packed_topic) + len(message) + 1

    if qos > 0:
        size += 2

    packed = pack_fixed_header(ControlPacketType.PUBLISH, qos << 1, size)
    packed += packed_topic

    if qos > 0:
        packed += struct.pack('>H', packet_identifier)

    packed += pack_variable_integer(0)
    packed += message

    return packed


def unpack_publish(payload, qos):
    topic = unpack_string(payload)

    if payload.is_data_available():
        properties = unpack_properties(
            'PUBLISH',
            [
                PropertyIds.PAYLOAD_FORMAT_INDICATOR,
                PropertyIds.MESSAGE_EXPIRY_INTERVAL,
                PropertyIds.CONTENT_TYPE,
                PropertyIds.RESPONSE_TOPIC,
                PropertyIds.CORRELATION_DATA,
                PropertyIds.SUBSCRIPTION_IDENTIFIER,
                PropertyIds.TOPIC_ALIAS,
                PropertyIds.USER_PROPERTY
            ],
            payload)

    if qos == 0:
        message = payload.read_all()
    else:
        payload.read(2)
        message = payload.read_all()

    return topic, message


def unpack_puback(payload):
    packet_identifier = unpack_u16(payload)

    if payload.is_data_available():
        reason = payload.read(1)[0]
    else:
        reason = 0

    try:
        reason = PubackReasonCode(reason)
    except ValueError:
        pass

    if payload.is_data_available():
        unpack_properties('PUBACK',
                          [
                              PropertyIds.REASON_STRING,
                              PropertyIds.USER_PROPERTY
                          ],
                          payload)

    return packet_identifier, reason


def unpack_pubrec(payload):
    packet_identifier = unpack_u16(payload)

    if payload.is_data_available():
        reason = payload.read(1)[0]
    else:
        reason = 0

    try:
        reason = PubrecReasonCode(reason)
    except ValueError:
        pass

    if payload.is_data_available():
        unpack_properties('PUBREC',
                          [
                              PropertyIds.REASON_STRING,
                              PropertyIds.USER_PROPERTY
                          ],
                          payload)

    return packet_identifier, reason


def unpack_pubcomp(payload):
    packet_identifier = unpack_u16(payload)

    if payload.is_data_available():
        reason = payload.read(1)[0]
    else:
        reason = 0

    try:
        reason = PubcompReasonCode(reason)
    except ValueError:
        pass

    if payload.is_data_available():
        unpack_properties('PUBCOMP',
                          [
                              PropertyIds.REASON_STRING,
                              PropertyIds.USER_PROPERTY
                          ],
                          payload)

    return packet_identifier, reason


def pack_pubrec(payload):
    packed = pack_fixed_header(ControlPacketType.PUBREC, 0, 2)
    packed += payload

    return packed


def pack_pubrel(packet_identifier, reason):
    packed = pack_fixed_header(ControlPacketType.PUBREL, 2, 3)
    packed += struct.pack('>HB', packet_identifier, reason)

    return packed


def pack_ping():
    return pack_fixed_header(ControlPacketType.PINGREQ, 0, 0)


class Transaction(object):

    def __init__(self, client):
        self.packet_identifier = None
        self._event = asyncio.Event()
        self._client = client
        self._response = None

    def __enter__(self):
        self.packet_identifier = self._client.alloc_packet_identifier()
        self._client.transactions[self.packet_identifier] = self

        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self.packet_identifier in self._client.transactions:
            del self._client.transactions[self.packet_identifier]

    async def wait_for_response(self):
        response = await self.wait_until_completed()
        self._event.clear()

        return response

    async def wait_until_completed(self):
        await asyncio.wait_for(self._event.wait(),
                               self._client.response_timeout)

        return self._response

    def set_response(self, response):
        self._response = response
        self._event.set()

    def set_completed(self, response):
        del self._client.transactions[self.packet_identifier]
        self.set_response(response)


class Client(object):
    """An MQTT client that reconnects on communication failure.

    """

    def __init__(self,
                 host,
                 port,
                 client_id=None,
                 will_topic='',
                 will_message=b'',
                 will_qos=0,
                 keep_alive_s=0,
                 response_timeout=50,
                 **kwargs):
        self._host = host
        self._port = port

        if client_id is None:
            client_id = 'mqttools-{}'.format(uuid.uuid1().hex[:14])

        self._client_id = client_id
        self._will_topic = will_topic
        self._will_message = will_message
        self._will_qos = will_qos
        self._keep_alive_s = keep_alive_s
        self._kwargs = kwargs
        self.response_timeout = response_timeout
        self._reader = None
        self._writer = None
        self._monitor_task = None
        self._reader_task = None
        self._keep_alive_task = None
        self._connack_event = asyncio.Event()
        self._pingresp_event = asyncio.Event()
        self.transactions = {}
        self._subscribed = set()
        self.messages = asyncio.Queue()
        self._connack = None
        self._next_packet_identifier = 1
        self._receive_maximum = None
        self._receive_maximum_semaphore = None

        if keep_alive_s == 0:
            self._ping_period_s = None
        else:
            self._ping_period_s = max(1, keep_alive_s - response_timeout - 1)

    @property
    def client_id(self):
        return self._client_id

    @property
    def receive_maximum(self):
        return self._receive_maximum

    async def start(self):
        self._reader, self._writer = await asyncio.open_connection(
            self._host,
            self._port,
            **self._kwargs)
        self._monitor_task = asyncio.create_task(self.monitor_main())
        self._reader_task = asyncio.create_task(self.reader_main())

        if self._keep_alive_s != 0:
            self._keep_alive_task = asyncio.create_task(self.keep_alive_main())

        await self.connect()

    async def stop(self):
        self.disconnect()
        self._monitor_task.cancel()

        try:
            await self._monitor_task
        except asyncio.CancelledError:
            pass

        self._reader_task.cancel()

        try:
            await self._reader_task
        except asyncio.CancelledError:
            pass

        if self._keep_alive_task is not None:
            self._keep_alive_task.cancel()

            try:
                await self._keep_alive_task
            except asyncio.CancelledError:
                pass

        self._writer.close()

    async def connect(self):
        self._connack_event.clear()
        self._write_packet(pack_connect(self._client_id,
                                        self._will_topic,
                                        self._will_message,
                                        self._will_qos,
                                        self._keep_alive_s))

        try:
            await asyncio.wait_for(self._connack_event.wait(),
                                   self.response_timeout)
        except asyncio.TimeoutError:
            raise Error('Timeout waiting for CONNACK from the broker.')

        _, reason, properties = self._connack

        if PropertyIds.RECEIVE_MAXIMUM in properties:
            self._receive_maximum = properties[PropertyIds.RECEIVE_MAXIMUM]
        else:
            self._receive_maximum = 65535

        self._receive_maximum_semaphore = asyncio.Semaphore(self._receive_maximum)

        if reason != ConnectReasonCode.SUCCESS:
            raise ConnectError(self._connect_reason)

    def disconnect(self):
        self._write_packet(pack_disconnect())

    async def subscribe(self, topic, qos):
        with Transaction(self) as transaction:
            self._write_packet(pack_subscribe(topic,
                                              qos,
                                              transaction.packet_identifier))
            await transaction.wait_until_completed()
            self._subscribed.add((topic, qos))

    def publish_qos_0(self, topic, message):
        with Transaction(self) as transaction:
            self._write_packet(pack_publish(topic,
                                            message,
                                            0,
                                            transaction.packet_identifier))

    async def publish_qos_1(self, topic, message):
        async with self._receive_maximum_semaphore:
            with Transaction(self) as transaction:
                self._write_packet(pack_publish(topic,
                                                message,
                                                1,
                                                transaction.packet_identifier))
                reason = await transaction.wait_until_completed()

                if reason != PubackReasonCode.SUCCESS:
                    raise PublishError(reason)

    async def publish_qos_2(self, topic, message):
        async with self._receive_maximum_semaphore:
            with Transaction(self) as transaction:
                self._write_packet(pack_publish(topic,
                                                message,
                                                2,
                                                transaction.packet_identifier))
                reason = await transaction.wait_for_response()

                if reason != PubrecReasonCode.SUCCESS:
                    raise PublishError(reason)

                self._write_packet(pack_pubrel(transaction.packet_identifier,
                                               PubrelReasonCode.SUCCESS))
                reason = await transaction.wait_until_completed()

                if reason != PubcompReasonCode.SUCCESS:
                    raise PublishError(reason)

    async def publish(self, topic, message, qos):
        if qos == 0:
            self.publish_qos_0(topic, message)
        elif qos == 1:
            await self.publish_qos_1(topic, message)
        elif qos == 2:
            await self.publish_qos_2(topic, message)
        else:
            raise Error(f'Invalid QoS {qos}.')

    def on_connack(self, payload):
        self._connack = unpack_connack(payload)
        self._connack_event.set()

    async def on_publish(self, flags, payload):
        qos = ((flags >> 1) & 0x3)

        try:
            unpacked = unpack_publish(payload, qos)
        except MalformedPacketError:
            LOGGER.debug('Discarding malformed PUBLISH packet.')
            return

        # ToDo: Add support for QoS 1 and 2.
        await self.messages.put(unpacked)

    def on_puback(self, payload):
        try:
            packet_identifier, reason = unpack_puback(payload)
        except MalformedPacketError:
            LOGGER.debug('Discarding malformed PUBACK packet.')
            return

        if packet_identifier in self.transactions:
            self.transactions[packet_identifier].set_completed(reason)
        else:
            LOGGER.debug(
                'Discarding unexpected PUBACK packet with identifier %d.',
                packet_identifier)

    def on_pubrec(self, payload):
        try:
            packet_identifier, reason = unpack_pubrec(payload)
        except MalformedPacketError:
            LOGGER.debug('Discarding malformed PUBREC packet.')
            return

        if packet_identifier in self.transactions:
            self.transactions[packet_identifier].set_response(reason)
        else:
            LOGGER.debug(
                'Discarding unexpected PUBREC packet with identifier %d.',
                packet_identifier)

    def on_pubcomp(self, payload):
        try:
            packet_identifier, reason = unpack_pubcomp(payload)
        except MalformedPacketError:
            LOGGER.debug('Discarding malformed PUBCOMP packet.')
            return

        if packet_identifier in self.transactions:
            self.transactions[packet_identifier].set_completed(reason)
        else:
            LOGGER.debug(
                'Discarding unexpected PUBCOMP packet with identifier %d.',
                packet_identifier)

    def on_suback(self, payload):
        try:
            packet_identifier, properties = unpack_suback(payload)
        except MalformedPacketError:
            LOGGER.debug('Discarding malformed SUBACK packet.')
            return

        if packet_identifier in self.transactions:
            self.transactions[packet_identifier].set_completed(None)
        else:
            LOGGER.debug(
                'Discarding unexpected SUBACK packet with identifier %d.',
                packet_identifier)

    def on_pingresp(self):
        self._pingresp_event.set()

    async def reconnect(self):
        LOGGER.warning('Reconnecting...')

        self._reader, self._writer = await asyncio.open_connection(
            self._host,
            self._port,
            **self._kwargs)
        await self.connect()

        for topic, qos in self._subscribed:
            await self.subscribe(topic, qos)

    async def monitor_loop(self):
        while True:
            if self._writer.is_closing():
                self._writer.close()
                await self.reconnect()

            await asyncio.sleep(1)

    async def monitor_main(self):
        """Monitor the broker connection and reconnect on failure.

        """

        try:
            await self.monitor_loop()
        except BaseException as e:
            LOGGER.info('Monitor task stopped by %r.', e)
            raise

    async def reader_loop(self):
        while True:
            try:
                packet_type, flags, payload = await self._read_packet()
            except asyncio.IncompleteReadError:
                LOGGER.info('Failed to read packet.')
                await asyncio.sleep(1)
                continue

            if packet_type == ControlPacketType.CONNACK:
                self.on_connack(payload)
            elif packet_type == ControlPacketType.PUBLISH:
                await self.on_publish(flags, payload)
            elif packet_type == ControlPacketType.PUBACK:
                self.on_puback(payload)
            elif packet_type == ControlPacketType.PUBREC:
                self.on_pubrec(payload)
            elif packet_type == ControlPacketType.PUBCOMP:
                self.on_pubcomp(payload)
            elif packet_type == ControlPacketType.SUBACK:
                self.on_suback(payload)
            elif packet_type == ControlPacketType.PINGRESP:
                self.on_pingresp()
            else:
                LOGGER.warning("Unsupported packet type %s with data %s.",
                               control_packet_type_to_string(packet_type),
                               payload.getvalue())

    async def reader_main(self):
        """Read packets from the broker.

        """

        try:
            await self.reader_loop()
        except BaseException as e:
            LOGGER.info('Reader task stopped by %r.', e)
            raise

    async def keep_alive_loop(self):
        while True:
            await asyncio.sleep(self._ping_period_s)

            LOGGER.debug('Pinging the broker.')

            self._pingresp_event.clear()
            self._write_packet(pack_ping())

            try:
                await asyncio.wait_for(self._pingresp_event.wait(),
                                       self.response_timeout)
            except asyncio.TimeoutError:
                LOGGER.warning('Timeout waiting for ping response.')

    async def keep_alive_main(self):
        """Ping the broker periodically to keep the connection alive.

        """

        try:
            await self.keep_alive_loop()
        except BaseException as e:
            LOGGER.info('Keep alive task stopped by %r.', e)
            raise

    def _write_packet(self, message):
        if LOGGER.isEnabledFor(logging.DEBUG):
            LOGGER.debug(
                "Sending %s packet %s.",
                control_packet_type_to_string(unpack_packet_type(message)),
                binascii.hexlify(message))

        self._writer.write(message)

    async def _read_packet(self):
        buf = await self._reader.readexactly(1)
        packet_type, flags = bitstruct.unpack('u4u4', buf)
        size = 0
        multiplier = 1
        byte = 0x80

        while (byte & 0x80) == 0x80:
            buf += await self._reader.readexactly(1)
            byte = buf[-1]
            size += ((byte & 0x7f) * multiplier)
            multiplier <<= 7

        data = await self._reader.readexactly(size)

        if LOGGER.isEnabledFor(logging.DEBUG):
            LOGGER.debug("Received %s packet %s.",
                         control_packet_type_to_string(packet_type),
                         binascii.hexlify(buf + data))

        return packet_type, flags, PayloadReader(data)

    def alloc_packet_identifier(self):
        packet_identifier = self._next_packet_identifier

        if packet_identifier in self.transactions:
            raise Error('No packet identifier available.')

        self._next_packet_identifier += 1

        if self._next_packet_identifier == 65536:
            self._next_packet_identifier = 1

        return packet_identifier
