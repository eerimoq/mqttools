import logging
import asyncio
import sys
import argparse
import struct
import bitstruct

from .version import __version__


# Control packet types.
CONNECT      = 1
CONNACK      = 2
PUBLISH      = 3
PUBACK       = 4
PUBREC       = 5
PUBREL       = 6
PUBCOMP      = 7
SUBSCRIBE    = 8
SUBACK       = 9
UNSUBSCRIBE  = 10
UNSUBACK     = 11
PINGREQ      = 12
PINGRESP     = 13
DISCONNECT   = 14

# Connection flags.
CLEAN_SESSION   = 0x02
WILL_FLAG       = 0x04
WILL_QOS_1      = 0x08
WILL_QOS_2      = 0x10
WILL_RETAIN     = 0x20
PASSWORD_FLAG   = 0x40
USER_NAME_FLAG  = 0x80

CONNECTION_ACCEPTED = 0

LOGGER = logging.getLogger(__name__)


class ConnectionError(Exception):

    pass


def pack_string(data):
    packed = struct.pack('>H', len(data))
    packed += data

    return packed


def pack_fixed_header(message_type, flags, size):
    packed = bitstruct.pack('u4u4', message_type, flags)

    if size == 0:
        packed += b'\x00'
    else:
        while size > 0:
            encoded_byte = (size & 0x7f)
            size >>= 7

            if size > 0:
                encoded_byte |= 0x80

            packed += struct.pack('B', encoded_byte)

    return packed


def pack_connect(client_id,
                 will_topic,
                 will_message,
                 will_qos,
                 keep_alive_s):
    if bool(len(will_topic)) != bool(len(will_message)):
        raise Exception()

    flags = CLEAN_SESSION
    payload_length = len(client_id) + 2

    if will_topic:
        flags |= WILL_FLAG

        if will_qos == 1:
            flags |= WILL_QOS_1
        elif will_qos == 2:
            flags |= WILL_QOS_2

        payload_length += len(will_topic) + 2
        payload_length += len(will_message) + 2

    packed = pack_fixed_header(CONNECT, 0, 10 + payload_length)
    packed += struct.pack('>H', 4)
    packed += b'MQTT'
    packed += struct.pack('>BBH', 4, flags, keep_alive_s)
    packed += pack_string(client_id)

    if will_topic:
        packed += pack_string(will_topic)

    if will_message:
        packed += pack_string(will_message)

    return packed


def pack_disconnect():
    return pack_fixed_header(DISCONNECT, 0, 0)


def pack_subscribe(topic, qos):
    packed = pack_fixed_header(SUBSCRIBE, 2, len(topic) + 5)
    packed += struct.pack('>BBH', 0, 1, len(topic))
    packed += topic
    packed += struct.pack('B', qos)

    return packed


def pack_publish(topic, message, qos):
    size = len(topic) + len(message) + 2

    if qos > 0:
        size += 2

    packed = pack_fixed_header(PUBLISH, qos << 1, size)
    packed += struct.pack('>H', len(topic))
    packed += topic

    if qos > 0:
        packed += b'\x00\x01'

    packed += message

    return packed


def unpack_publish(payload, qos):
    size = struct.unpack('>H', payload[0:2])[0]
    topic = payload[2:2 + size]

    if qos == 0:
        message = payload[2 + size:]
    else:
        message = payload[2 + size + 2:]

    return topic, message


def unpack_puback(payload):
    return struct.unpack('>H', payload)[0]


def pack_pubrel():
    packed = pack_fixed_header(PUBREL, 2, 2)
    packed += b'\x00\x01'

    return packed


def pack_ping():
    return pack_fixed_header(PINGREQ, 0, 0)


class Client(object):
    """An MQTT client that reconnects on communication failure.

    """

    def __init__(self,
                 host,
                 port,
                 client_id,
                 will_topic=b'',
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
        self._response_timeout = response_timeout
        self._reader = None
        self._writer = None
        self._monitor_task = None
        self._reader_task = None
        self._keep_alive_task = None
        self._connack_event = asyncio.Event()
        self._suback_event = asyncio.Event()
        self._pingresp_event = asyncio.Event()
        self._subscribed = set()

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
                               self._response_timeout)

    def disconnect(self):
        self._write_packet(pack_disconnect())

    async def subscribe(self, topic, qos):
        self._suback_event.clear()
        self._write_packet(pack_subscribe(topic, qos))
        await asyncio.wait_for(self._suback_event.wait(),
                               self._response_timeout)
        self._subscribed.add((topic, qos))

    async def publish(self, topic, message, qos):
        self._write_packet(pack_publish(topic, message, qos))

    def on_connack(self):
        self._connack_event.set()

    def on_publish(self, flags, payload):
        qos = ((flags >> 1) & 0x3)
        self.on_message(*unpack_publish(payload, qos))

    def on_message(self, topic, message):
        LOGGER.info('Received message - Topic: %s, Message: %s', topic, message)

    def on_suback(self):
        self._suback_event.set()

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

            await asyncio.sleep(5)

    async def reader_main(self):
        """Read packets from the broker.

        """

        while True:
            try:
                packet_type, flags, payload = await self._read_packet()
            except ConnectionError:
                LOGGER.info('Failed to read packet.')
                await asyncio.sleep(1)
                continue

            if packet_type == CONNACK:
                self.on_connack()
            elif packet_type == PUBLISH:
                self.on_publish(flags, payload)
            elif packet_type == SUBACK:
                self.on_suback()
            elif packet_type == PINGRESP:
                self.on_pingresp()
            else:
                LOGGER.warning('Unsupported packet type %d.', packet_type)

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
                                           self._response_timeout)
                except asyncio.TimeoutError:
                    LOGGER.warning('Timeout waiting for ping response.')

    def _write_packet(self, message):
        LOGGER.debug('Sending packet %s to the broker.', message)

        self._writer.write(message)

    async def _read_packet(self):
        buf = await self._reader.read(1)

        if len(buf) != 1:
            raise ConnectionError()

        packet_type, flags = bitstruct.unpack('u4u4', buf)
        size = 0
        multiplier = 1
        byte = 0x80

        while (byte & 0x80) == 0x80:
            buf = await self._reader.read(1)

            if len(buf) != 1:
                raise ConnectionError()

            byte = buf[0]
            size += ((byte & 0x7f) * multiplier)
            multiplier <<= 7

        data = await self._reader.read(size)

        if len(data) != size:
            raise ConnectionError()

        LOGGER.debug('Received packet %s from the broker.', data)

        return packet_type, flags, data


class SubscriberClient(Client):

    def on_message(self, topic, message):
        print(f'Topic:   {topic}')
        print(f'Message: {message}')
        print()


async def subscriber(host, port, topic, qos):
    client = SubscriberClient(host, port, b'mqttools_subscribe')

    await client.start()
    await client.subscribe(topic, qos)

    while True:
        await asyncio.sleep(1)


async def publisher(host, port, topic, message, qos):
    client = Client(host, port, b'mqttools_publish')

    await client.start()

    print(f'Topic:   {topic}')
    print(f'Message: {message}')
    print(f'QoS:     {qos}')

    await client.publish(topic, message, qos)
    await client.stop()


def _do_subscribe(args):
    asyncio.run(subscriber(args.host,
                           args.port,
                           args.topic.encode('ascii'),
                           args.qos))


def _do_publish(args):
    asyncio.run(publisher(args.host,
                          args.port,
                          args.topic.encode('ascii'),
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
        args.func(args)
    else:
        try:
            args.func(args)
        except BaseException as e:
            sys.exit('error: ' + str(e))
