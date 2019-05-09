import asyncio
import bitstruct
import logging
import binascii
from collections import defaultdict
import enum

from .common import ControlPacketType
from .common import DisconnectReasonCode
from .common import MalformedPacketError
from .common import PayloadReader
from .common import control_packet_type_to_string
from .common import unpack_packet_type
from .common import unpack_connect
from .common import pack_connack
from .common import pack_publish
from .common import unpack_publish
from .common import unpack_subscribe
from .common import pack_suback
from .common import unpack_unsubscribe
from .common import pack_unsuback
from .common import unpack_disconnect


LOGGER = logging.getLogger(__name__)


class Session(object):

    def __init__(self):
        self.subscribes = set()
        self.expiry_time = None

    def clean(self):
        self.subscribes = set()
        self.expiry_time = None

class Client(object):

    def __init__(self, broker, reader, writer):
        self._broker = broker
        self._reader = reader
        self._writer = writer
        self._disconnect_reason = DisconnectReasonCode.NORMAL_DISCONNECTION
        self._session = None

    async def serve_forever(self):
        addr = self._writer.get_extra_info('peername')

        LOGGER.info('Serving client %r.', addr)

        try:
            packet_type, _, payload = await self.read_packet()

            if packet_type == ControlPacketType.CONNECT:
                self.on_connect(payload)
            else:
                raise MalformedPacketError(
                    f'Unsupported or invalid packet type {packet_type}.')

            await self.reader_loop()
        except Exception as e:
            LOGGER.debug('Reader task stopped by %r.', e)

            if isinstance(e, MalformedPacketError):
                self._disconnect_reason = DisconnectReasonCode.MALFORMED_PACKET

        LOGGER.info('Closing client %r.', addr)

    async def reader_loop(self):
        while True:
            packet_type, flags, payload = await self.read_packet()

            if packet_type == ControlPacketType.PUBLISH:
                self.on_publish(payload)
            elif packet_type == ControlPacketType.SUBSCRIBE:
                self.on_subscribe(payload)
            elif packet_type == ControlPacketType.UNSUBSCRIBE:
                self.on_unsubscribe(payload)
            elif packet_type == ControlPacketType.DISCONNECT:
                self.on_disconnect(payload)
            else:
                raise MalformedPacketError(
                    f'Unsupported or invalid packet type {packet_type}.')

    async def read_packet(self):
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

    def on_connect(self, payload):
        client_id, clean_start, keep_alive_s, properties = unpack_connect(
            payload)
        self._session, session_present = self._broker.get_session(
            client_id,
            clean_start)
        self._write_packet(pack_connack(session_present, 0, {}))

    def on_publish(self, payload):
        topic, message, _ = unpack_publish(payload, 0)

        for client in self._broker.iter_subscribers(topic):
            client.publish(topic, message)

    def on_subscribe(self, payload):
        topic, packet_identifier = unpack_subscribe(payload)
        self._session.subscribes.add(topic)
        self._broker.add_subscriber(topic, self._session)
        self._write_packet(pack_suback(packet_identifier))

    def on_unsubscribe(self, payload):
        topic, packet_identifier = unpack_unsubscribe(payload)
        self._session.subscribes.remove(topic)
        self._broker.remove_subscriber(topic, self._session)
        self._write_packet(pack_unsuback(packet_identifier))

    def on_disconnect(self, payload):
        unpack_disconnect(payload)

        raise Exception()

    def publish(self, topic, message):
        self._write_packet(pack_publish(topic, message, None))

    def _write_packet(self, message):
        if LOGGER.isEnabledFor(logging.DEBUG):
            LOGGER.debug(
                "Sending %s packet %s.",
                control_packet_type_to_string(unpack_packet_type(message)),
                binascii.hexlify(message))

        self._writer.write(message)


class Broker(object):
    """An MQTT 5.0 broker.

    `host` and `port` are the host and port to listen for clients on.

    """

    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._sessions = {}
        self._subscribers = defaultdict(list)

    async def run(self):
        listener = await asyncio.start_server(self.serve_client,
                                              self._host,
                                              self._port)
        listener_address = listener.sockets[0].getsockname()

        LOGGER.info(f'Listening for clients on {listener_address}.')

        async with listener:
            await listener.serve_forever()

    async def serve_client(self, reader, writer):
        client = Client(self, reader, writer)
        await client.serve_forever()

    def add_subscriber(self, topic, session):
        topic_sessions = self._subscribers[topic]

        if session not in topic_sessions:
            topic_sessions.append(session)

    def remove_subscriber(self, topic, session):
        topic_sessions = self._subscribers[topic]

        if session in topic_sessions:
            del topic_sessions[topic_sessions.index(session)]

    def iter_subscribers(self, topic):
        topic_sessions = self._subscribers[topic]

        if session in topic_sessions:
            if topic in session.subscribes:
                yield session
            else:
                del topic_sessions[topic_sessions.index(session)]

    def get_session(self, client_id, clean_start):
        session_present = False

        if client_id in self._sessions:
            session = self._sessions[client_id]

            if clean_start:
                for topic in session.subscribes:
                    self.remove_subscriber(topic, session)

                session.clean()
            else:
                session_present = True
        else:
            session = Session()
            self._sessions[client_id] = session

        return session, session_present
