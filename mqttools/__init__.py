import logging
import asyncio
import sys
import argparse
import struct
import enum
import bitstruct
from io import BytesIO

from .version import __version__


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
    SUCCESS                       = 0
    UNSPECIFIED_ERROR             = 128
    MALFORMED_PACKET              = 129
    PROTOCOL_ERROR                = 130
    IMPLEMENTATION_SPECIFIC_ERROR = 131
    UNSUPPORTED_PROTOCOL_VERSION  = 132
    CLIENT_IDENTIFIER_NOT_VALID   = 133
    BAD_USER_NAME_OR_PASSWORD     = 134
    NOT_AUTHORIZED                = 135
    SERVER_UNAVAILABLE            = 136
    SERVER_BUSY                   = 137
    BANNED                        = 138
    BAD_AUTHENTICATION_METHOD     = 140
    TOPIC_NAME_INVALID            = 144
    PACKET_TOO_LARGE              = 149
    QUOTA_EXCEEDED                = 151
    PAYLOAD_FORMAT_INVALID        = 153
    RETAIN_NOT_SUPPORTED          = 154
    QOS_NOT_SUPPORTED             = 155
    USE_ANOTHER_SERVER            = 156
    SERVER_MOVED                  = 157
    CONNECTION_RATE_EXCEEDED      = 159


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


# MQTT 5.0
PROTOCOL_VERSION = 5

LOGGER = logging.getLogger(__name__)


class Error(Exception):
    pass


class ConnectError(Exception):

    def __init__(self, reason):
        super().__init__()
        self.reason = reason


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


def pack_binary(data):
    packed = struct.pack('>H', len(data))
    packed += data

    return packed


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
        byte = payload.read(1)[0]
        value += ((byte & 0x7f) * multiplier)
        multiplier <<= 7

    return value


def pack_fixed_header(message_type, flags, size):
    packed = bitstruct.pack('u4u4', message_type, flags)
    packed += pack_variable_integer(size)

    return packed


def pack_connect(client_id,
                 will_topic,
                 will_message,
                 will_qos,
                 keep_alive_s):
    if bool(len(will_topic)) != bool(len(will_message)):
        raise Error('Bad will.')

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
    flags = struct.unpack('>B', payload.read(1))[0]
    session_present = bool(flags & 1)

    try:
        reason = ConnectReasonCode(payload.read(1)[0])
    except ValueError:
        pass

    return session_present, reason


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
    packet_identifier = struct.unpack('>H', payload.read(2))[0]

    return packet_identifier


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
    size = struct.unpack('>H', payload.read(2))[0]
    topic = payload.read(size).decode('utf-8')
    props_size = unpack_variable_integer(payload)
    payload.read(props_size)

    if qos == 0:
        message = payload.read()
    else:
        payload.read(2)
        message = payload.read()

    return topic, message


def unpack_puback(payload):
    return struct.unpack('>H', payload.read(2))[0]


def pack_pubrec(payload):
    packed = pack_fixed_header(ControlPacketType.PUBREC, 0, 2)
    packed += payload

    return packed


def pack_pubrel():
    packed = pack_fixed_header(ControlPacketType.PUBREL, 2, 2)
    packed += b'\x00\x01'

    return packed


def pack_ping():
    return pack_fixed_header(ControlPacketType.PINGREQ, 0, 0)


class Transaction(object):

    def __init__(self, client):
        self.packet_identifier = None
        self._event = asyncio.Event()
        self._client = client

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

    def mark_completed(self):
        del self._client.transactions[self.packet_identifier]
        self._event.set()


class Client(object):
    """An MQTT client that reconnects on communication failure.

    """

    def __init__(self,
                 host,
                 port,
                 client_id,
                 will_topic='',
                 will_message=b'',
                 will_qos=0,
                 keep_alive_s=0,
                 response_timeout=5):
        self._host = host
        self._port = port
        self._client_id = client_id
        self._will_topic = will_topic
        self._will_message = will_message
        self._will_qos = will_qos
        self._keep_alive_s = keep_alive_s
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
        self._connect_reason = None
        self._next_packet_identifier = 1

    async def start(self):
        self._reader, self._writer = await asyncio.open_connection(
            self._host,
            self._port)
        self._monitor_task = asyncio.create_task(self.monitor_main())
        self._reader_task = asyncio.create_task(self.reader_main())
        self._keep_alive_task = asyncio.create_task(self.keep_alive_main())
        await self.connect()

    async def stop(self):
        self.disconnect()
        self._monitor_task.cancel()
        self._reader_task.cancel()
        self._keep_alive_task.cancel()

        try:
            await self._monitor_task
        except asyncio.CancelledError:
            pass

        try:
            await self._reader_task
        except asyncio.CancelledError:
            pass

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
        await asyncio.wait_for(self._connack_event.wait(),
                               self.response_timeout)

        if self._connect_reason != ConnectReasonCode.SUCCESS:
            raise ConnectError(reason)

    def disconnect(self):
        self._write_packet(pack_disconnect())

    async def subscribe(self, topic, qos):
        with Transaction(self) as transaction:
            self._write_packet(pack_subscribe(topic,
                                              qos,
                                              transaction.packet_identifier))
            await transaction.wait_until_completed()
            self._subscribed.add((topic, qos))

    async def publish(self, topic, message, qos):
        if qos > 0:
            raise Error('Only QoS 0 is supported.')

        with Transaction(self) as transaction:
            self._write_packet(pack_publish(topic,
                                            message,
                                            qos,
                                            transaction.packet_identifier))

    def on_connack(self, payload):
        _, self._connect_reason = unpack_connack(payload)
        self._connack_event.set()

    async def on_publish(self, flags, payload):
        qos = ((flags >> 1) & 0x3)
        await self.messages.put(unpack_publish(payload, qos))

    def on_suback(self, payload):
        packet_identifier = unpack_suback(payload)

        if packet_identifier in self.transactions:
            self.transactions[packet_identifier].mark_completed()
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
            self._port)
        await self.connect()

        for topic, qos in self._subscribed:
            await self.subscribe(topic, qos)

    async def monitor_main(self):
        """Monitor the broker connection and reconnect on failure.

        """

        while True:
            if self._writer.is_closing():
                self._writer.close()
                await self.reconnect()

            await asyncio.sleep(1)

    async def reader_main(self):
        """Read packets from the broker.

        """

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
            elif packet_type == ControlPacketType.SUBACK:
                self.on_suback(payload)
            elif packet_type == ControlPacketType.PINGRESP:
                self.on_pingresp()
            else:
                LOGGER.warning("Unsupported packet type %s with data %s.",
                               control_packet_type_to_string(packet_type),
                               payload.getvalue())

    async def keep_alive_main(self):
        """Ping the broker periodically to keep the connection alive.

        """

        while True:
            if self._keep_alive_s == 0:
                await asyncio.sleep(1)
            else:
                await asyncio.sleep(self._keep_alive_s / 2)
                self._pingresp_event.clear()
                self._write_packet(pack_ping())

                try:
                    await asyncio.wait_for(self._pingresp_event.wait(),
                                           self.response_timeout)
                except asyncio.TimeoutError:
                    LOGGER.warning('Timeout waiting for ping response.')

    def _write_packet(self, message):
        LOGGER.debug('Sending packet %s to the broker.', message)

        self._writer.write(message)

    async def _read_packet(self):
        buf = await self._reader.readexactly(1)
        packet_type, flags = bitstruct.unpack('u4u4', buf)
        size = 0
        multiplier = 1
        byte = 0x80

        while (byte & 0x80) == 0x80:
            buf = await self._reader.readexactly(1)
            byte = buf[0]
            size += ((byte & 0x7f) * multiplier)
            multiplier <<= 7

        data = await self._reader.readexactly(size)

        LOGGER.debug('Received packet %s from the broker.', data)

        return packet_type, flags, BytesIO(data)

    def alloc_packet_identifier(self):
        packet_identifier = self._next_packet_identifier

        if packet_identifier in self.transactions:
            raise Error('No packet identifier available.')

        self._next_packet_identifier += 1

        if self._next_packet_identifier == 65536:
            self._next_packet_identifier = 1

        return packet_identifier


async def subscriber(host, port, topic, qos):
    client = Client(host, port, 'mqttools_subscribe')

    await client.start()
    await client.subscribe(topic, qos)

    while True:
        topic, message = await client.messages.get()

        print(f'Topic:   {topic}')
        print(f'Message: {message}')


async def publisher(host, port, topic, message, qos):
    client = Client(host, port, 'mqttools_publish')

    await client.start()

    print(f'Topic:   {topic}')
    print(f'Message: {message}')
    print(f'QoS:     {qos}')

    await client.publish(topic, message, qos)
    await client.stop()


def _do_subscribe(args):
    asyncio.run(subscriber(args.host,
                           args.port,
                           args.topic,
                           args.qos))


def _do_publish(args):
    asyncio.run(publisher(args.host,
                          args.port,
                          args.topic,
                          args.message.encode('ascii'),
                          args.qos))


def main():
    parser = argparse.ArgumentParser(description='MQTT Tools.')

    parser.add_argument('-d', '--debug', action='store_true')
    parser.add_argument('--version',
                        action='version',
                        version=__version__,
                        help='Print version information and exit.')

    # Workaround to make the subparser required in Python 3.
    subparsers = parser.add_subparsers(title='subcommands',
                                       dest='subcommand')
    subparsers.required = True

    # Subscribe subparser.
    subparser = subparsers.add_parser('subscribe',
                                      description='Subscribe for given topic.')
    subparser.add_argument('--host',
                           default='test.mosquitto.org',
                           help='Broker host (default: test.mosquitto.org).')
    subparser.add_argument('--port',
                           type=int,
                           default=1883,
                           help='Broker port (default: 1883).')
    subparser.add_argument('--qos',
                           type=int,
                           default=0,
                           help='Quality of service (default: 0).')
    subparser.add_argument('topic', help='Topic to subscribe for.')
    subparser.set_defaults(func=_do_subscribe)

    # Publish subparser.
    subparser = subparsers.add_parser('publish',
                                      description='Publish given topic.')
    subparser.add_argument('--host',
                           default='test.mosquitto.org',
                           help='Broker host (default: test.mosquitto.org).')
    subparser.add_argument('--port',
                           type=int,
                           default=1883,
                           help='Broker port (default: 1883).')
    subparser.add_argument('--qos',
                           type=int,
                           default=0,
                           help='Quality of service (default: 0).')
    subparser.add_argument('topic', help='Topic to publish.')
    subparser.add_argument('message', help='Message to publish.')
    subparser.set_defaults(func=_do_publish)

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)

    if args.debug:
        args.func(args)
    else:
        try:
            args.func(args)
        except BaseException as e:
            sys.exit('error: ' + str(e))
