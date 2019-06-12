import re
import asyncio
import logging
from collections import defaultdict

from .common import ControlPacketType
from .common import ConnectReasonCode
from .common import SubackReasonCode
from .common import UnsubackReasonCode
from .common import DisconnectReasonCode
from .common import PropertyIds
from .common import MalformedPacketError
from .common import PayloadReader
from .common import unpack_connect
from .common import pack_connack
from .common import pack_publish
from .common import unpack_publish
from .common import unpack_subscribe
from .common import pack_suback
from .common import unpack_unsubscribe
from .common import pack_unsuback
from .common import pack_pingresp
from .common import pack_disconnect
from .common import unpack_disconnect
from .common import format_packet
from .common import CF_FIXED_HEADER
from .common import MAXIMUM_PACKET_SIZE


LOGGER = logging.getLogger(__name__)


# ToDo: Session expiry. Keep alive.


class ConnectError(Exception):
    pass


class DisconnectError(Exception):
    pass


class ProtocolError(Exception):
    pass


class Session(object):

    def __init__(self, client_id):
        self.client_id = client_id
        self.subscriptions = set()
        self.wildcard_subscriptions = set()
        self.expiry_time = None
        self.client = None
        self.maximum_packet_size = MAXIMUM_PACKET_SIZE

    def clean(self):
        self.subscriptions = set()
        self.wildcard_subscriptions = set()
        self.expiry_time = None
        self.client = None
        self.maximum_packet_size = MAXIMUM_PACKET_SIZE


def is_wildcards_in_topic(topic):
    return '#' in topic or '+' in topic


class Client(object):

    def __init__(self, broker, reader, writer):
        self._broker = broker
        self._reader = reader
        self._writer = writer
        self._session = None
        self._disconnect_reason = DisconnectReasonCode.NORMAL_DISCONNECTION

    async def serve_forever(self):
        addr = self._writer.get_extra_info('peername')

        self.log_info('Serving client %r.', addr)

        try:
            packet_type, _, payload = await self.read_packet()

            if packet_type == ControlPacketType.CONNECT:
                self.on_connect(payload)
            else:
                raise ConnectError()

            await self.reader_loop()
        except (ConnectError, DisconnectError):
            pass
        except asyncio.IncompleteReadError:
            self.log_debug('Client connection lost.')
        except Exception as e:
            self.log_debug('Reader task stopped by %r.', e)

            if isinstance(e, MalformedPacketError):
                self._disconnect_reason = DisconnectReasonCode.MALFORMED_PACKET
            elif isinstance(e, ProtocolError):
                self._disconnect_reason = DisconnectReasonCode.PROTOCOL_ERROR

            self.disconnect()

        if self._session is not None:
            self._session.client = None

        self.log_info('Closing client %r.', addr)

    async def reader_loop(self):
        while True:
            packet_type, flags, payload = await self.read_packet()

            if packet_type == ControlPacketType.PUBLISH:
                self.on_publish(payload)
            elif packet_type == ControlPacketType.SUBSCRIBE:
                self.on_subscribe(payload)
            elif packet_type == ControlPacketType.UNSUBSCRIBE:
                self.on_unsubscribe(payload)
            elif packet_type == ControlPacketType.PINGREQ:
                self.on_pingreq()
            elif packet_type == ControlPacketType.DISCONNECT:
                self.on_disconnect(payload)
            else:
                raise ProtocolError()

    async def read_packet(self):
        buf = await self._reader.readexactly(1)
        packet_type, flags = CF_FIXED_HEADER.unpack(buf)
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
            for line in format_packet('Received', buf + data):
                self.log_debug(line)

        return packet_type, flags, PayloadReader(data)

    def on_connect(self, payload):
        (client_id,
         clean_start,
         keep_alive_s,
         properties,
         user_name,
         password) = unpack_connect(payload)
        self._session, session_present = self._broker.get_session(
            client_id,
            clean_start)

        if session_present:
            self.log_info(
                'Session resumed with %d simple and %d wildcard '
                'subscriptions.',
                len(self._session.subscriptions),
                len(self._session.wildcard_subscriptions))

        self._session.client = self
        reason = ConnectReasonCode.SUCCESS

        if PropertyIds.AUTHENTICATION_METHOD in properties:
            reason = ConnectReasonCode.BAD_AUTHENTICATION_METHOD

        if PropertyIds.MAXIMUM_PACKET_SIZE in properties:
            maximum_packet_size = properties[PropertyIds.MAXIMUM_PACKET_SIZE]
            self._session.maximum_packet_size = maximum_packet_size

        if (user_name is not None) or (password is not None):
            reason = ConnectReasonCode.BAD_USER_NAME_OR_PASSWORD

        self._write_packet(pack_connack(
            session_present,
            reason,
            {
                PropertyIds.MAXIMUM_QOS: 0,
                PropertyIds.RETAIN_AVAILABLE: 0,
                PropertyIds.WILDCARD_SUBSCRIPTION_AVAILABLE: 0,
                PropertyIds.SHARED_SUBSCRIPTION_AVAILABLE: 0
            }))

        if reason != ConnectReasonCode.SUCCESS:
            raise ConnectError()

        self.log_info('Client connected.')

    def on_publish(self, payload):
        topic, message, _ = unpack_publish(payload, 0)

        if is_wildcards_in_topic(topic):
            raise MalformedPacketError(f'Invalid topic {topic} in publish.')

        for session in self._broker.iter_subscribers(topic):
            session.client.publish(topic, message)

    def on_subscribe(self, payload):
        packet_identifier, _, subscriptions = unpack_subscribe(payload)
        reasons = bytearray()

        for topic, _ in subscriptions:
            if is_wildcards_in_topic(topic):
                if topic not in self._session.wildcard_subscriptions:
                    self._session.wildcard_subscriptions.add(topic)
                    self._broker.add_wildcard_subscriber(topic, self._session)
            elif topic not in self._session.subscriptions:
                self._session.subscriptions.add(topic)
                self._broker.add_subscriber(topic, self._session)

            reason = SubackReasonCode.GRANTED_QOS_0
            reasons.append(reason)

        self._write_packet(pack_suback(packet_identifier, reasons))

    def on_unsubscribe(self, payload):
        packet_identifier, topics = unpack_unsubscribe(payload)
        reasons = bytearray()

        for topic in topics:
            reason = UnsubackReasonCode.NO_SUBSCRIPTION_EXISTED

            if is_wildcards_in_topic(topic):
                if topic in self._session.wildcard_subscriptions:
                    self._session.wildcard_subscriptions.remove(topic)
                    self._broker.remove_wildcard_subscriber(topic, self._session)
                    reason = UnsubackReasonCode.SUCCESS
            elif topic in self._session.subscriptions:
                self._session.subscriptions.remove(topic)
                self._broker.remove_subscriber(topic, self._session)
                reason = UnsubackReasonCode.SUCCESS

            reasons.append(reason)

        self._write_packet(pack_unsuback(packet_identifier, reasons))

    def on_pingreq(self):
        self._write_packet(pack_pingresp())

    def on_disconnect(self, payload):
        unpack_disconnect(payload)

        raise DisconnectError()

    def publish(self, topic, message):
        self._write_packet(pack_publish(topic, message, None))

    def disconnect(self):
        self._write_packet(pack_disconnect(self._disconnect_reason))

    def _write_packet(self, message):
        if LOGGER.isEnabledFor(logging.DEBUG):
            if len(message) <= self._session.maximum_packet_size:
                prefix = 'Sending'
            else:
                prefix = 'Not sending'

            for line in format_packet(prefix, message):
                self.log_debug(line)

        if len(message) <= self._session.maximum_packet_size:
            self._writer.write(message)

    def log_debug(self, fmt, *args):
        if LOGGER.isEnabledFor(logging.DEBUG):
            if self._session is None:
                LOGGER.debug(fmt, *args)
            else:
                LOGGER.debug(f'{self._session.client_id} {fmt}', *args)

    def log_info(self, fmt, *args):
        if LOGGER.isEnabledFor(logging.INFO):
            if self._session is None:
                LOGGER.info(fmt, *args)
            else:
                LOGGER.info(f'{self._session.client_id} {fmt}', *args)


class Broker(object):
    """A limited MQTT version 5.0 broker.

    `host` and `port` are the host and port to listen for clients on.

    """

    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._sessions = {}
        self._subscribers = defaultdict(list)
        self._wildcard_subscribers = []
        self._listener = None
        self._listener_ready = asyncio.Event()

    async def getsockname(self):
        await self._listener_ready.wait()

        return self._listener.sockets[0].getsockname()

    async def serve_forever(self):
        """Setup a listener socket and forever serve clients. This coroutine
        only ends if cancelled by the user.

        """

        self._listener = await asyncio.start_server(self.serve_client,
                                                    self._host,
                                                    self._port)
        self._listener_ready.set()
        listener_address = self._listener.sockets[0].getsockname()

        LOGGER.info('Listening for clients on %s.', listener_address)

        async with self._listener:
            await self._listener.serve_forever()

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

    def add_wildcard_subscriber(self, topic, session):
        pattern = topic.replace('+', '[^/]*')
        pattern = pattern.replace('/#', '.*')
        pattern = pattern.replace('#', '.*')
        pattern = '^' + pattern + '$'
        self._wildcard_subscribers.append((topic, session, re.compile(pattern)))

    def remove_wildcard_subscriber(self, topic, session):
        for index, subscriber in enumerate(self._wildcard_subscribers):
            if topic == subscriber[0] and session == subscriber[1]:
                del self._wildcard_subscribers[index]
                break

    def iter_subscribers(self, topic):
        for session in self._subscribers[topic]:
            if session.client is not None:
                yield session

        for _, session, re_topic in self._wildcard_subscribers:
            if session.client is not None:
                mo = re_topic.match(topic)

                if mo:
                    yield session

    def get_session(self, client_id, clean_start):
        session_present = False

        if client_id in self._sessions:
            session = self._sessions[client_id]

            if clean_start:
                for topic in session.subscriptions:
                    self.remove_subscriber(topic, session)

                for topic in session.wildcard_subscriptions:
                    self.remove_wildcard_subscriber(topic, session)

                session.clean()
            else:
                session_present = True
        else:
            session = Session(client_id)
            self._sessions[client_id] = session

        return session, session_present
