import re
import asyncio
import logging
import threading
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
from .common import format_packet_compact
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


class NotRunningError(Exception):
    pass


class Session(object):

    def __init__(self, client_id):
        self.client_id = client_id
        self.subscriptions = set()
        self.wildcard_subscriptions = set()
        self.expiry_interval = 0
        self.client = None
        self.maximum_packet_size = MAXIMUM_PACKET_SIZE
        self.will_topic = None
        self.will_message = None

    def clean(self):
        self.subscriptions = set()
        self.wildcard_subscriptions = set()
        self.expiry_interval = 0
        self.client = None
        self.maximum_packet_size = MAXIMUM_PACKET_SIZE
        self.will_topic = None
        self.will_message = None


def is_wildcards_in_topic(topic):
    return '#' in topic or '+' in topic


def compile_wildcards_topic(topic):
    pattern = topic.replace('+', '[^/]*')
    pattern = pattern.replace('/#', '.*')
    pattern = pattern.replace('#', '.*')
    pattern = '^' + pattern + '$'

    return re.compile(pattern)


class Client(object):

    def __init__(self, broker, reader, writer):
        self._broker = broker
        self._reader = reader
        self._writer = writer
        self._session = None
        self._disconnect_reason = DisconnectReasonCode.UNSPECIFIED_ERROR

    async def serve_forever(self):
        addr = self._writer.get_extra_info('peername')

        self.log_info('Serving client %s:%d.', *addr)

        try:
            packet_type, _, payload = await self.read_packet()

            if packet_type == ControlPacketType.CONNECT:
                self.on_connect(payload)
            else:
                raise ConnectError()

            await self.reader_loop()
        except ConnectError:
            pass
        except DisconnectError:
            self._disconnect_reason = DisconnectReasonCode.NORMAL_DISCONNECTION
        except asyncio.IncompleteReadError:
            self.log_debug('Client connection lost.')
        except Exception as e:
            self.log_debug('Reader task stopped by %r.', e)

            if isinstance(e, MalformedPacketError):
                self._disconnect_reason = DisconnectReasonCode.MALFORMED_PACKET
            elif isinstance(e, ProtocolError):
                self._disconnect_reason = DisconnectReasonCode.PROTOCOL_ERROR

            if self._session is not None:
                self.disconnect()

        if self._session is not None:
            self._session.client = None

            if self._session.will_topic is not None:
                if not self.is_normal_disconnection():
                    self._broker.publish(self._session.will_topic,
                                         self._session.will_message)

                    if self._session.will_retain:
                        self._broker.add_retained_message(self._session.will_topic,
                                                          self._session.will_message)

            if self._session.expiry_interval == 0:
                self._broker.remove_session(self._session.client_id)

        self.log_info('Closing client %r.', addr)

    async def reader_loop(self):
        while True:
            packet_type, flags, payload = await self.read_packet()

            if packet_type == ControlPacketType.PUBLISH:
                self.on_publish(payload, flags)
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
        elif LOGGER.isEnabledFor(logging.INFO):
            self.log_info(format_packet_compact('Received', buf + data))

        return packet_type, flags, PayloadReader(data)

    def on_connect(self, payload):
        (client_id,
         clean_start,
         will_topic,
         will_message,
         will_retain,
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

        self._session.will_topic = will_topic
        self._session.will_message = will_message
        self._session.will_retain = will_retain

        if PropertyIds.SESSION_EXPIRY_INTERVAL in properties:
            session_expiry_interval = properties[PropertyIds.SESSION_EXPIRY_INTERVAL]
            self._session.expiry_interval = session_expiry_interval

        if (user_name is not None) or (password is not None):
            reason = ConnectReasonCode.BAD_USER_NAME_OR_PASSWORD

        self._write_packet(pack_connack(
            session_present,
            reason,
            {
                PropertyIds.MAXIMUM_QOS: 0,
                PropertyIds.WILDCARD_SUBSCRIPTION_AVAILABLE: 0,
                PropertyIds.SHARED_SUBSCRIPTION_AVAILABLE: 0
            }))

        if reason != ConnectReasonCode.SUCCESS:
            raise ConnectError()

        self.log_info('Client connected.')

    def on_publish(self, payload, flags):
        topic, message, _ = unpack_publish(payload, (flags >> 1) & 3)

        if is_wildcards_in_topic(topic):
            raise MalformedPacketError(f'Invalid topic {topic} in publish.')

        if flags & 1:
            if message:
                self._broker.add_retained_message(topic, message)
            else:
                self._broker.remove_retained_message(topic)

        self._broker.publish(topic, message)

    def on_subscribe(self, payload):
        packet_identifier, _, subscriptions = unpack_subscribe(payload)
        reasons = bytearray()
        retained_messages = []

        for topic, _ in subscriptions:
            if is_wildcards_in_topic(topic):
                if topic not in self._session.wildcard_subscriptions:
                    self._session.wildcard_subscriptions.add(topic)
                    self._broker.add_wildcard_subscriber(topic, self._session)

                retained_messages += list(
                    self._broker.find_retained_messages_wildcards(topic))
            else:
                if topic not in self._session.subscriptions:
                    self._session.subscriptions.add(topic)
                    self._broker.add_subscriber(topic, self._session)

                retained_message = self._broker.find_retained_message(topic)

                if retained_message:
                    retained_messages.append(retained_message)

            reason = SubackReasonCode.GRANTED_QOS_0
            reasons.append(reason)

        self._write_packet(pack_suback(packet_identifier, reasons))

        for topic, message in retained_messages:
            self.publish(topic, message)

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
        self._write_packet(pack_publish(topic, message, False, None))

    def disconnect(self):
        self._write_packet(pack_disconnect(self._disconnect_reason))

    def _send_prefix(self, message):
        if len(message) <= self._session.maximum_packet_size:
            return 'Sending'
        else:
            return 'Not sending'

    def _write_packet(self, message):
        if LOGGER.isEnabledFor(logging.DEBUG):
            for line in format_packet(self._send_prefix(message), message):
                self.log_debug(line)
        elif LOGGER.isEnabledFor(logging.INFO):
            self.log_info(format_packet_compact(self._send_prefix(message),
                                                message))

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

    def is_normal_disconnection(self):
        return self._disconnect_reason == DisconnectReasonCode.NORMAL_DISCONNECTION


class Server:

    def __init__(self, serve_client, address):
        self.serve_client = serve_client

        if len(address) == 3:
            self._ssl = address[2]
        else:
            self._ssl = None

        self._host = address[0]
        self._port = address[1]
        self.server = None
        self.ready = asyncio.Event()

    async def serve_forever(self):
        try:
            self.server = await asyncio.start_server(self.serve_client,
                                                     self._host,
                                                     self._port,
                                                     ssl=self._ssl)
        except OSError as e:
            LOGGER.warning('%s', e)
            raise

        self.ready.set()
        server_address = self.server.sockets[0].getsockname()

        LOGGER.info('Listening for clients on %s.', server_address)

        async with self.server:
            await self.server.serve_forever()


class Broker(object):
    """A limited MQTT version 5.0 broker.

    `addresses` is a list of ``(host, port)`` and ``(host, port,
    ssl)`` tuples. It may also be the host string or one of the
    tuples. The broker will listen for clients on all given
    addresses. ``ssl`` is an SSL context passed to
    `asyncio.start_server()` as `ssl`.

    Create a broker and serve clients:

    >>> broker = Broker('localhost')
    >>> await broker.serve_forever()

    """

    def __init__(self, addresses):
        if isinstance(addresses, str):
            addresses = (addresses, 1883)

        if isinstance(addresses, tuple):
            addresses = [addresses]

        self._sessions = {}
        self._subscribers = defaultdict(list)
        self._wildcard_subscribers = []
        self._servers = []

        for address in addresses:
            self._servers.append(Server(self.serve_client, address))

        self._client_tasks = set()
        self._retained_messages = {}

    async def getsockname(self, index=0):
        server = self._servers[index]
        await server.ready.wait()

        return server.server.sockets[0].getsockname()

    async def serve_forever(self):
        """Setup a listener socket and forever serve clients. This coroutine
        only ends if cancelled by the user.

        """

        try:
            await asyncio.gather(
                *[server.serve_forever() for server in self._servers])
        except asyncio.CancelledError:
            # Cancel all client tasks as the TCP server leaves them
            # running.
            for client_task in self._client_tasks:
                client_task.cancel()

            self._client_tasks = set()
            raise

    async def serve_client(self, reader, writer):
        current_task = asyncio.current_task()
        self._client_tasks.add(current_task)
        client = Client(self, reader, writer)

        try:
            await client.serve_forever()
        finally:
            try:
                self._client_tasks.remove(current_task)
            except KeyError:
                pass

    def add_subscriber(self, topic, session):
        topic_sessions = self._subscribers[topic]

        if session not in topic_sessions:
            topic_sessions.append(session)

    def remove_subscriber(self, topic, session):
        topic_sessions = self._subscribers[topic]

        if session in topic_sessions:
            del topic_sessions[topic_sessions.index(session)]

    def add_wildcard_subscriber(self, topic, session):
        re_topic = compile_wildcards_topic(topic)
        self._wildcard_subscribers.append((topic, session, re_topic))

    def remove_wildcard_subscriber(self, topic, session):
        for index, subscriber in enumerate(self._wildcard_subscribers):
            if topic == subscriber[0] and session == subscriber[1]:
                del self._wildcard_subscribers[index]
                break

    def add_retained_message(self, topic, message):
        self._retained_messages[topic] = message

    def remove_retained_message(self, topic):
        try:
            del self._retained_messages[topic]
        except KeyError:
            pass

    def find_retained_messages_wildcards(self, topic):
        re_topic = compile_wildcards_topic(topic)

        for topic in self._retained_messages:
            mo = re_topic.match(topic)

            if mo:
                yield (topic, self._retained_messages[topic])

    def find_retained_message(self, topic):
        if topic in self._retained_messages:
            return (topic, self._retained_messages[topic])
        else:
            return None

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

    def remove_session(self, client_id):
        del self._sessions[client_id]

    def  publish(self, topic, message):
        """Publish given topic and message to all subscribers.

        """

        for session in self.iter_subscribers(topic):
            session.client.publish(topic, message)


class BrokerThread(threading.Thread):
    """The same as :class:`Broker`, but running in a thread.

    Create a broker and serve clients for 60 seconds:

    >>> broker = BrokerThread('broker.hivemq.com')
    >>> broker.start()
    >>> time.sleep(60)
    >>> broker.stop()

    """

    def __init__(self, addresses):
        super().__init__()
        self._addresses = addresses
        self.daemon = True
        self._loop = asyncio.new_event_loop()
        self._broker_task = self._loop.create_task(self._run())
        self._running = False

    def run(self):
        asyncio.set_event_loop(self._loop)
        self._running = True

        try:
            self._loop.run_until_complete(self._broker_task)
        finally:
            self._loop.close()

    def stop(self):
        """Stop the broker. All clients will be disconnected and the thread
        will terminate.

        """

        if not self._running:
            raise NotRunningError('The broker is already stopped.')

        self._running = False

        def cancel_broker_task():
            self._broker_task.cancel()

        self._loop.call_soon_threadsafe(cancel_broker_task)
        self.join()

    async def _run(self):
        broker = Broker(self._addresses)

        try:
            await broker.serve_forever()
        except asyncio.CancelledError:
            pass
