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


class QoS(enum.IntEnum):
    """Quality of service levels.

    """

    AT_MOST_ONCE  = 0
    "At most once (QoS 0)."

    AT_LEAST_ONCE = 1
    "At least once (QoS 1)."

    EXACTLY_ONCE  = 2
    "Exactly once (QoS 2)."


# MQTT 5.0
PROTOCOL_VERSION = 5

LOGGER = logging.getLogger(__name__)


class Error(Exception):
    pass


class MalformedPacketError(Error):
    pass


class ConnectError(Error):

    def __init__(self, reason):
        super().__init__()
        self.reason = reason

    def __str__(self):
        message = f'{self.reason.name}({self.reason.value})'

        if self.reason == ConnectReasonCode.V3_1_1_UNACCEPTABLE_PROTOCOL_VERSION:
            message += ': The broker does not support protocol version 5.'

        return message


class SessionResumeError(Error):
    pass


class PublishError(Error):

    def __init__(self, reason):
        super().__init__()
        self.reason = reason

    def __str__(self):
        if isinstance(self.reason, enum.Enum):
            return f'{self.reason.name}({self.reason.value})'
        else:
            return f'UNKNOWN({self.reason})'



class PayloadReader(BytesIO):

    def read(self, size):
        data = super().read(size)

        if len(data) != size:
            raise MalformedPacketError('Payload too short.')

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


def pack_property(property_id, value):
    return struct.pack('B', property_id) + {
        PropertyIds.PAYLOAD_FORMAT_INDICATOR: pack_u8,
        PropertyIds.MESSAGE_EXPIRY_INTERVAL: pack_u32,
        PropertyIds.CONTENT_TYPE: pack_string,
        PropertyIds.RESPONSE_TOPIC: pack_string,
        PropertyIds.CORRELATION_DATA: pack_binary,
        PropertyIds.SUBSCRIPTION_IDENTIFIER: pack_variable_integer,
        PropertyIds.SESSION_EXPIRY_INTERVAL: pack_u32,
        PropertyIds.ASSIGNED_CLIENT_IDENTIFIER: pack_string,
        PropertyIds.SERVER_KEEP_ALIVE: pack_u16,
        PropertyIds.AUTHENTICATION_METHOD: pack_string,
        PropertyIds.AUTHENTICATION_DATA: pack_binary,
        PropertyIds.REQUEST_PROBLEM_INFORMATION: pack_u8,
        PropertyIds.WILL_DELAY_INTERVAL: pack_u32,
        PropertyIds.REQUEST_RESPONSE_INFORMATION: pack_u8,
        PropertyIds.RESPONSE_INFORMATION: pack_string,
        PropertyIds.SERVER_REFERENCE: pack_string,
        PropertyIds.REASON_STRING: pack_string,
        PropertyIds.RECEIVE_MAXIMUM: pack_u16,
        PropertyIds.TOPIC_ALIAS_MAXIMUM: pack_u16,
        PropertyIds.TOPIC_ALIAS: pack_u16,
        PropertyIds.MAXIMUM_QOS: pack_u8,
        PropertyIds.RETAIN_AVAILABLE: pack_u8,
        PropertyIds.USER_PROPERTY: pack_string,
        PropertyIds.MAXIMUM_PACKET_SIZE: pack_u32,
        PropertyIds.WILDCARD_SUBSCRIPTION_AVAILABLE: pack_u8,
        PropertyIds.SUBSCRIPTION_IDENTIFIER_AVAILABLE: pack_u8,
        PropertyIds.SHARED_SUBSCRIPTION_AVAILABLE: pack_u8
    }[property_id](value)


def log_properties(packet_name, properties):
    if LOGGER.isEnabledFor(logging.DEBUG):
        LOGGER.debug('%s properties:', packet_name)

        for identifier, value in properties.items():
            LOGGER.debug('  %s(%d): %s',
                         identifier.name,
                         identifier.value,
                         value)


def pack_properties(packet_name, properties):
    log_properties(packet_name, properties)
    packed = b''

    for property_id, value in properties.items():
        packed += pack_property(property_id, value)

    return pack_variable_integer(len(packed)) + packed


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
    """Return a dictionary of unpacked properties.

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

    log_properties(packet_name, properties)

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
                 clean_start,
                 will_topic,
                 will_message,
                 will_qos,
                 keep_alive_s,
                 properties):
    flags = 0

    if clean_start:
        flags |= CLEAN_START

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

    properties = pack_properties('CONNECT', properties)
    packed = pack_fixed_header(ControlPacketType.CONNECT,
                               0,
                               10 + payload_length + len(properties))
    packed += struct.pack('>H', 4)
    packed += b'MQTT'
    packed += struct.pack('>BBH',
                          PROTOCOL_VERSION,
                          flags,
                          keep_alive_s)
    packed += properties
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


def pack_disconnect(reason):
    packed = pack_fixed_header(ControlPacketType.DISCONNECT, 0, 2)
    packed += struct.pack('B', reason)
    packed += pack_variable_integer(0)

    return packed


def unpack_disconnect(payload):
    if payload.is_data_available():
        reason = payload.read(1)[0]
    else:
        reason = 0

    try:
        reason = DisconnectReasonCode(reason)
    except ValueError:
        pass

    if payload.is_data_available():
        properties = unpack_properties(
            'DISCONNECT',
            [
                PropertyIds.SESSION_EXPIRY_INTERVAL,
                PropertyIds.SERVER_REFERENCE,
                PropertyIds.REASON_STRING,
                PropertyIds.USER_PROPERTY
            ],
            payload)
    else:
        properties = {}

    return reason, properties


def pack_subscribe(topic, packet_identifier):
    packed_topic = pack_string(topic)
    packed = pack_fixed_header(ControlPacketType.SUBSCRIBE,
                               2,
                               len(packed_topic) + 4)
    packed += struct.pack('>HB', packet_identifier, 0)
    packed += packed_topic
    packed += struct.pack('B', 0)

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


def pack_unsubscribe(topic, packet_identifier):
    packed_topic = pack_string(topic)
    packed = pack_fixed_header(ControlPacketType.UNSUBSCRIBE,
                               2,
                               len(packed_topic) + 3)
    packed += struct.pack('>HB', packet_identifier, 0)
    packed += packed_topic

    return packed


def unpack_unsuback(payload):
    packet_identifier = unpack_u16(payload)
    properties = unpack_properties('UNSUBACK',
                                   [
                                       PropertyIds.REASON_STRING,
                                       PropertyIds.USER_PROPERTY
                                   ],
                                   payload)

    return packet_identifier, properties


def pack_publish(topic, message, alias):
    if alias is None:
        properties = b'\x00'
    else:
        properties = pack_properties('PUBLISH',
                                     {PropertyIds.TOPIC_ALIAS: alias})

    packed_topic = pack_string(topic)
    size = len(packed_topic) + len(message) + len(properties)
    packed = pack_fixed_header(ControlPacketType.PUBLISH, 0, size)
    packed += packed_topic
    packed += properties
    packed += message

    return packed


def unpack_publish(payload, qos):
    topic = unpack_string(payload)

    if qos > 0:
        raise MalformedPacketError('Only QoS 0 is supported.')

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

    message = payload.read_all()

    return topic, message, properties


def pack_pingreq():
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

    async def wait_until_completed(self):
        await asyncio.wait_for(self._event.wait(),
                               self._client.response_timeout)

        return self._response

    def set_completed(self, response):
        del self._client.transactions[self.packet_identifier]
        self._response = response
        self._event.set()


class Client(object):
    """An MQTT 5.0 client.

    `host` and `port` are the host and port of the broker.

    `client_id` is the client id string. If ``None``, a random client
    id is generated on the form ``mqttools-<UUID[0..14]>``.

    `will_topic`, `will_message` and `will_qos` are used to ask the
    broker to send a will when the session ends.

    `keep_alive_s` is the keep alive time in seconds.

    `response_timeout` is the maximum time to wait for a response from
    the broker.

    `topic_aliases` is a list of topics that should be published with
    aliases instead of the topic string.

    `topic_alias_maximum` is the maximum number of topic aliases the
    client is willing to assign on request from the broker.

    `session_expiry_interval` is the session expiry time in
    seconds. Give as 0 to remove the session in the broker when the
    connection ends.

    `kwargs` are passed to ``asyncio.open_connection()``.

    Create a client with default configuration:

    >>> client = Client('broker.hivemq.com', 1883)

    Create a client with using all optional arguments:

    >>> client = Client('broker.hivemq.com',
                        1883,
                        client_id='my-client',
                        will_topic='/my/last/will',
                        will_message=b'my-last-message',
                        will_qos=QoS.EXACTLY_ONCE,
                        keep_alive_s=600,
                        response_timeout=30',
                        topic_aliases=['/my/topic']',
                        topic_alias_maximum=100,
                        session_expiry_interval=1800,
                        ssl=True)

    """

    def __init__(self,
                 host,
                 port,
                 client_id=None,
                 will_topic='',
                 will_message=b'',
                 will_qos=QoS.AT_MOST_ONCE,
                 keep_alive_s=0,
                 response_timeout=5,
                 topic_aliases=None,
                 topic_alias_maximum=0,
                 session_expiry_interval=0,
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
        self._connect_properties = {}
        self._topic_alias_maximum = topic_alias_maximum
        self._topic_aliases = None

        if topic_alias_maximum > 0:
            self._connect_properties[PropertyIds.TOPIC_ALIAS_MAXIMUM] = (
                topic_alias_maximum)

        if session_expiry_interval > 0:
            self._connect_properties[PropertyIds.SESSION_EXPIRY_INTERVAL] = (
                session_expiry_interval)

        if topic_aliases is None:
            topic_aliases = []

        self._broker_topic_aliases = None
        self._broker_topic_aliases_init = {
            topic: alias
            for alias, topic in enumerate(topic_aliases, 1)
        }
        self._broker_topic_alias_maximum = None
        self._registered_broker_topic_aliases = None
        self._reader = None
        self._writer = None
        self._reader_task = None
        self._keep_alive_task = None
        self._connack_event = None
        self._pingresp_event = None
        self.transactions = None
        self._messages = None
        self._connack = None
        self._next_packet_identifier = None
        self._disconnect_reason = None

        if keep_alive_s == 0:
            self._ping_period_s = None
        else:
            self._ping_period_s = max(1, keep_alive_s - response_timeout - 1)

    @property
    def client_id(self):
        """The client identifier string.

        """

        return self._client_id

    @property
    def messages(self):
        """An ``asyncio.Queue`` of received messages from the broker. Each
        message is a topic-message tuple.

        >>> await client.messages.get()
        ('/my/topic', b'my-message')

        A ``(None, None)`` message is put in the queue when the broker
        connection is lost.

        >>> await client.messages.get()
        (None, None)

        """

        return self._messages

    async def start(self, resume_session=False):
        """Open a TCP connection to the broker and perform the MQTT connect
        procedure. This method must be called before any `publish()`
        or `subscribe()` calls. Call `stop()` to close the connection.

        If `resume_session` is ``True``, the client tries to resume
        the last session in the broker. A `SessionResumeError`
        exception is raised if the resume fails, and a new session has
        been created instead.

        >>> await client.start()

        >>> try:
        ...     await client.start()
        ...     print('Session resumed.')
        ... except SessionResumeError:
        ...     print('Session not resumed. Subscribe to topics.')

        """

        self._topic_aliases = {}
        self._broker_topic_aliases = {}
        self._broker_topic_alias_maximum = 0
        self._registered_broker_topic_aliases = set()
        self._connack_event = asyncio.Event()
        self._pingresp_event = asyncio.Event()
        self.transactions = {}
        self._messages = asyncio.Queue()
        self._connack = None
        self._next_packet_identifier = 1
        self._disconnect_reason = DisconnectReasonCode.NORMAL_DISCONNECTION

        LOGGER.info('Connecting to %s:%s.', self._host, self._port)

        self._reader, self._writer = await asyncio.open_connection(
            self._host,
            self._port,
            **self._kwargs)
        self._reader_task = asyncio.create_task(self._reader_main())

        if self._keep_alive_s != 0:
            self._keep_alive_task = asyncio.create_task(self._keep_alive_main())
        else:
            self._keep_alive_task = None

        await self.connect(resume_session)

    async def stop(self):
        """Try to cleanly disconnect from the broker and then close the TCP
        connection. Call `start()` after `stop()` to reconnect to the
        broker.

        >>> await client.stop()

        """

        try:
            self.disconnect()
        except Exception:
            pass

        self._reader_task.cancel()

        try:
            await self._reader_task
        except Exception:
            pass

        if self._keep_alive_task is not None:
            self._keep_alive_task.cancel()

            try:
                await self._keep_alive_task
            except Exception:
                pass

        self._writer.close()

    async def connect(self, resume_session):
        self._connack_event.clear()
        self._write_packet(pack_connect(self._client_id,
                                        not resume_session,
                                        self._will_topic,
                                        self._will_message,
                                        self._will_qos,
                                        self._keep_alive_s,
                                        self._connect_properties))

        try:
            await asyncio.wait_for(self._connack_event.wait(),
                                   self.response_timeout)
        except asyncio.TimeoutError:
            raise Error('Timeout waiting for CONNACK from the broker.')

        session_present, reason, properties = self._connack

        if reason != ConnectReasonCode.SUCCESS:
            raise ConnectError(reason)

        # Topic alias maximum.
        if PropertyIds.TOPIC_ALIAS_MAXIMUM in properties:
            self._broker_topic_alias_maximum = (
                properties[PropertyIds.TOPIC_ALIAS_MAXIMUM])
        else:
            self._broker_topic_alias_maximum = 0

        if len(self._broker_topic_aliases_init) > self._broker_topic_alias_maximum:
            LOGGER.warning('The broker topic alias maximum is %d, which is lower '
                           'than the topic aliases length %d.',
                           self._broker_topic_alias_maximum,
                           len(self._broker_topic_aliases_init))

        self._broker_topic_aliases = {
            topic: alias
            for topic, alias in self._broker_topic_aliases_init.items()
            if alias < self._broker_topic_alias_maximum + 1
        }

        if resume_session and not session_present:
            LOGGER.info('No session to resume.')

            raise SessionResumeError('No session to resume.')

    def disconnect(self):
        if self._disconnect_reason is None:
            return

        self._write_packet(pack_disconnect(self._disconnect_reason))
        self._disconnect_reason = None

    async def subscribe(self, topic):
        """Subscribe to given topic with QoS 0.

        >>> await client.subscribe('/my/topic')
        >>> await client.messages.get()
        ('/my/topic', b'my-message')

        """

        with Transaction(self) as transaction:
            self._write_packet(pack_subscribe(topic,
                                              transaction.packet_identifier))
            await transaction.wait_until_completed()

    async def unsubscribe(self, topic):
        """Unsubscribe from given topic.

        >>> await client.unsubscribe('/my/topic')

        """

        with Transaction(self) as transaction:
            self._write_packet(pack_unsubscribe(topic,
                                                transaction.packet_identifier))
            await transaction.wait_until_completed()

    def publish(self, topic, message):
        """Publish given message to given topic with QoS 0.

        >>> client.publish('/my/topic', b'my-message')

        """

        if topic in self._broker_topic_aliases:
            alias = self._broker_topic_aliases[topic]

            if alias in self._registered_broker_topic_aliases:
                topic = ''
        else:
            alias = None

        self._write_packet(pack_publish(topic, message, alias))

        if (alias is not None) and (topic != ''):
            self._registered_broker_topic_aliases.add(alias)

    def on_connack(self, payload):
        self._connack = unpack_connack(payload)
        self._connack_event.set()

    async def on_publish(self, flags, payload):
        topic, message, properties = unpack_publish(
            payload,
            (flags >> 1) & 0x3)

        if PropertyIds.TOPIC_ALIAS in properties:
            alias = properties[PropertyIds.TOPIC_ALIAS]

            if topic == '':
                try:
                    topic = self._topic_aliases[alias]
                except KeyError:
                    LOGGER.debug(
                        'Invalid topic alias %d received from the broker.',
                        alias)
                    return
            elif 0 < alias <= self._topic_alias_maximum:
                self._topic_aliases[alias] = topic
            else:
                LOGGER.debug('Invalid topic alias %d received from the broker.',
                             alias)
                return

        await self._messages.put((topic, message))

    def on_suback(self, payload):
        packet_identifier, properties = unpack_suback(payload)

        if packet_identifier in self.transactions:
            self.transactions[packet_identifier].set_completed(None)
        else:
            LOGGER.debug(
                'Discarding unexpected SUBACK packet with identifier %d.',
                packet_identifier)

    def on_unsuback(self, payload):
        packet_identifier, properties = unpack_unsuback(payload)

        if packet_identifier in self.transactions:
            self.transactions[packet_identifier].set_completed(None)
        else:
            LOGGER.debug(
                'Discarding unexpected UNSUBACK packet with identifier %d.',
                packet_identifier)

    def on_pingresp(self):
        self._pingresp_event.set()

    async def on_disconnect(self, payload):
        reason, properties = unpack_disconnect(payload)

        if reason != DisconnectReasonCode.NORMAL_DISCONNECTION:
            LOGGER.info("Abnormal disconnect reason %s.", reason)

        if PropertyIds.REASON_STRING in properties:
            reason_string = properties[PropertyIds.REASON_STRING]
            LOGGER.info("Disconnect reason string '%s'.", reason_string)

        await self._close()

    async def reader_loop(self):
        while True:
            packet_type, flags, payload = await self._read_packet()

            if packet_type == ControlPacketType.CONNACK:
                self.on_connack(payload)
            elif packet_type == ControlPacketType.PUBLISH:
                await self.on_publish(flags, payload)
            elif packet_type == ControlPacketType.SUBACK:
                self.on_suback(payload)
            elif packet_type == ControlPacketType.UNSUBACK:
                self.on_unsuback(payload)
            elif packet_type == ControlPacketType.PINGRESP:
                self.on_pingresp()
            elif packet_type == ControlPacketType.DISCONNECT:
                await self.on_disconnect(payload)
            else:
                raise MalformedPacketError(
                    f'Unsupported or invalid packet type {packet_type}.')

    async def _reader_main(self):
        """Read packets from the broker.

        """

        try:
            await self.reader_loop()
        except Exception as e:
            LOGGER.info('Reader task stopped by %r.', e)

            if isinstance(e, MalformedPacketError):
                self._disconnect_reason = DisconnectReasonCode.MALFORMED_PACKET

            await self._close()

    async def keep_alive_loop(self):
        while True:
            await asyncio.sleep(self._ping_period_s)

            LOGGER.debug('Pinging the broker.')

            self._pingresp_event.clear()
            self._write_packet(pack_pingreq())
            await asyncio.wait_for(self._pingresp_event.wait(),
                                   self.response_timeout)

    async def _keep_alive_main(self):
        """Ping the broker periodically to keep the connection alive.

        """

        try:
            await self.keep_alive_loop()
        except Exception as e:
            LOGGER.info('Keep alive task stopped by %r.', e)
            await self._close()

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

    async def _close(self):
        self.disconnect()
        self._writer.close()
        await self._messages.put((None, None))
