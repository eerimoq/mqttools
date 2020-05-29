import logging
import asyncio
import enum
import uuid

from .common import ControlPacketType
from .common import ConnectReasonCode
from .common import PropertyIds
from .common import SubackReasonCode
from .common import UnsubackReasonCode
from .common import DisconnectReasonCode
from .common import pack_connect
from .common import unpack_connack
from .common import pack_disconnect
from .common import unpack_disconnect
from .common import pack_publish
from .common import unpack_publish
from .common import pack_subscribe
from .common import unpack_suback
from .common import pack_unsubscribe
from .common import unpack_unsuback
from .common import pack_pingreq
from .common import Error
from .common import MalformedPacketError
from .common import TimeoutError
from .common import PayloadReader
from .common import format_packet
from .common import format_packet_compact
from .common import CF_FIXED_HEADER


LOGGER = logging.getLogger(__name__)


class SessionResumeError(Error):
    pass


class ReasonError(Error):

    def __init__(self, reason):
        super().__init__()
        self.reason = reason

    def __str__(self):
        return f'{self.reason.name}({self.reason.value})'


class ConnectError(ReasonError):

    def __str__(self):
        message = f'{self.reason.name}({self.reason.value})'

        if self.reason == ConnectReasonCode.V3_1_1_UNACCEPTABLE_PROTOCOL_VERSION:
            message += ': The broker does not support protocol version 5.'

        return message


class SubscribeError(ReasonError):
    pass


class UnsubscribeError(ReasonError):
    pass


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
    """An MQTT version 5.0 client.

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
    seconds. Give as 0 to remove the session when the connection
    ends. Give as 0xffffffff to never remove the session (given that
    the broker supports it).

    `subscriptions` is a list of topics to subscribe to after
    connected in :meth:`start()`.

    `connect_delays` is a list of delays in seconds between the
    connection attempts in :meth:`start()`. Each delay is used once,
    except the last delay, which is used until successfully
    connected. If ``[]``, only one connection attempt is performed. If
    ``None``, the default delays ``[1, 2, 4, 8]`` are used.

    `kwargs` are passed to ``asyncio.open_connection()``.

    Create a client with default configuration:

    >>> client = Client('broker.hivemq.com', 1883)

    Create a client with using all optional arguments:

    >>> client = Client('broker.hivemq.com',
                        1883,
                        client_id='my-client',
                        will_topic='/my/last/will',
                        will_message=b'my-last-message',
                        will_qos=0,
                        keep_alive_s=600,
                        response_timeout=30',
                        topic_aliases=['/my/topic']',
                        topic_alias_maximum=100,
                        session_expiry_interval=1800,
                        subscriptions=['a/b', 'test/#'],
                        connect_delays=[1, 2],
                        ssl=True)

    """

    def __init__(self,
                 host,
                 port,
                 client_id=None,
                 will_topic='',
                 will_message=b'',
                 will_retain=False,
                 will_qos=0,
                 keep_alive_s=60,
                 response_timeout=5,
                 topic_aliases=None,
                 topic_alias_maximum=10,
                 session_expiry_interval=0,
                 subscriptions=None,
                 connect_delays=None,
                 **kwargs):
        self._host = host
        self._port = port

        if client_id is None:
            client_id = 'mqttools-{}'.format(uuid.uuid1().hex[:14])

        self._client_id = client_id
        self._will_topic = will_topic
        self._will_message = will_message
        self._will_retain = will_retain
        self._will_qos = will_qos
        self._keep_alive_s = keep_alive_s
        self._kwargs = kwargs
        self.response_timeout = response_timeout
        self._connect_properties = {}
        self._rx_topic_alias_maximum = topic_alias_maximum
        self._rx_topic_aliases = None

        if topic_alias_maximum > 0:
            self._connect_properties[PropertyIds.TOPIC_ALIAS_MAXIMUM] = (
                topic_alias_maximum)

        if session_expiry_interval > 0:
            self._connect_properties[PropertyIds.SESSION_EXPIRY_INTERVAL] = (
                session_expiry_interval)

        if subscriptions is None:
            subscriptions = []

        self._subscriptions = subscriptions

        if connect_delays is None:
            connect_delays = [1, 2, 4, 8]

        self._connect_delays = connect_delays

        if topic_aliases is None:
            topic_aliases = []

        self._tx_topic_aliases = None
        self._tx_topic_aliases_init = {
            topic: alias
            for alias, topic in enumerate(topic_aliases, 1)
        }
        self._tx_topic_alias_maximum = None
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
            self._ping_period_s = keep_alive_s

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

    async def on_message(self, topic, message):
        """Called for each received MQTT message and when the broker
        connection is lost. Puts the message on the messages queue by
        default.

        """

        await self._messages.put((topic, message))

    async def start(self, resume_session=False):
        """Open a TCP connection to the broker and perform the MQTT connect
        procedure. This method must be called before any
        :meth:`publish()` or :meth:`subscribe()` calls. Call
        :meth:`stop()` to close the connection.

        If `resume_session` is ``True``, the client tries to resume
        the last session in the broker. A :class:`SessionResumeError`
        exception is raised if the resume fails, and a new session has
        been created instead.

        The exceptions below are only raised if ``connect_delays`` is
        ``[]``.

        Raises ``ConnectionRefusedError`` if the TCP connection
        attempt is refused by the broker.

        Raises :class:`TimeoutError` if the broker does not
        acknowledge the connect request.

        Raises :class:`ConnectError` if the broker does not accept the
        connect request.

        Raises :class:`SubscribeError` if the broker does not accept
        the subscribe request(s).

        >>> await client.start()

        Trying to resume a session.

        >>> try:
        ...     await client.start(resume_session=True)
        ...     print('Session resumed.')
        ... except SessionResumeError:
        ...     print('Session not resumed. Subscribe to topics.')

        """

        attempt = 1
        delays = self._connect_delays

        while True:
            try:
                await self._start(resume_session)
                break
            except SessionResumeError:
                raise
            except (Exception, asyncio.CancelledError) as e:
                if isinstance(e, ConnectionRefusedError):
                    LOGGER.info('TCP connect refused.')
                elif isinstance(e, TimeoutError):
                    LOGGER.info(
                        'MQTT connect or subscribe acknowledge not received.')
                elif isinstance(e, ConnectError):
                    LOGGER.info('MQTT connect failed with reason %s.', e)
                elif isinstance(e, SubscribeError):
                    LOGGER.info('MQTT subscribe failed with reason %s.', e)

                if delays == []:
                    raise

            # Delay a while before the next connect attempt.
            delay = delays[min(attempt, len(delays)) - 1]
            LOGGER.info(
                'Waiting %s second(s) before next connection attempt(%d).',
                delay,
                attempt)
            await asyncio.sleep(delay)
            attempt += 1

    async def _start(self, resume_session=False):
        self._rx_topic_aliases = {}
        self._tx_topic_aliases = {}
        self._tx_topic_alias_maximum = 0
        self._registered_broker_topic_aliases = set()
        self._reader_task = None
        self._keep_alive_task = None
        self._connack_event = asyncio.Event()
        self._pingresp_event = asyncio.Event()
        self.transactions = {}
        self._messages = asyncio.Queue()
        self._connack = None
        self._next_packet_identifier = 1
        self._disconnect_reason = DisconnectReasonCode.NORMAL_DISCONNECTION

        LOGGER.info('Connecting to %s:%s.', self._host, self._port)

        try:
            self._reader, self._writer = await asyncio.open_connection(
                self._host,
                self._port,
                **self._kwargs)

            self._reader_task = asyncio.create_task(self._reader_main())

            session_present = await self.connect(resume_session)

            if self._keep_alive_s != 0:
                self._keep_alive_task = asyncio.create_task(self._keep_alive_main())

            if not resume_session or not session_present:
                for topic in self._subscriptions:
                    await self.subscribe(topic)
        except (Exception, asyncio.CancelledError):
            await self.stop()
            raise

        if resume_session and not session_present:
            LOGGER.info('No session to resume.')

            raise SessionResumeError('No session to resume.')

    async def stop(self):
        """Try to cleanly disconnect from the broker and then close the TCP
        connection. Call :meth:`start()` after :meth:`stop()` to
        reconnect to the broker.

        >>> await client.stop()

        """

        try:
            self.disconnect()
        except (Exception, asyncio.CancelledError):
            pass

        if self._reader_task is not None:
            self._reader_task.cancel()

            try:
                await self._reader_task
            except (Exception, asyncio.CancelledError):
                pass

        if self._keep_alive_task is not None:
            self._keep_alive_task.cancel()

            try:
                await self._keep_alive_task
            except (Exception, asyncio.CancelledError):
                pass

        if self._writer is not None:
            self._writer.close()

    async def connect(self, resume_session):
        self._connack_event.clear()
        self._write_packet(pack_connect(self._client_id,
                                        not resume_session,
                                        self._will_topic,
                                        self._will_message,
                                        self._will_retain,
                                        self._will_qos,
                                        self._keep_alive_s,
                                        self._connect_properties))

        try:
            await asyncio.wait_for(self._connack_event.wait(),
                                   self.response_timeout)
        except asyncio.TimeoutError:
            raise TimeoutError('Timeout waiting for CONNACK from the broker.')

        session_present, reason, properties = self._connack

        if reason != ConnectReasonCode.SUCCESS:
            raise ConnectError(reason)

        # Topic alias maximum.
        if PropertyIds.TOPIC_ALIAS_MAXIMUM in properties:
            self._tx_topic_alias_maximum = (
                properties[PropertyIds.TOPIC_ALIAS_MAXIMUM])
        else:
            self._tx_topic_alias_maximum = 0

        if len(self._tx_topic_aliases_init) > self._tx_topic_alias_maximum:
            LOGGER.warning('The broker topic alias maximum is %d, which is lower '
                           'than the topic aliases length %d.',
                           self._tx_topic_alias_maximum,
                           len(self._tx_topic_aliases_init))

        self._tx_topic_aliases = {
            topic: alias
            for topic, alias in self._tx_topic_aliases_init.items()
            if alias < self._tx_topic_alias_maximum + 1
        }

        return session_present

    def disconnect(self):
        if self._disconnect_reason is None:
            return

        self._write_packet(pack_disconnect(self._disconnect_reason))
        self._disconnect_reason = None

    async def subscribe(self, topic):
        """Subscribe to given topic with QoS 0.

        Raises :class:`TimeoutError` if the broker does not
        acknowledge the subscribe request.

        Raises :class:`SubscribeError` if the broker does not accept
        the subscribe request.

        >>> await client.subscribe('/my/topic')
        >>> await client.messages.get()
        ('/my/topic', b'my-message')

        """

        with Transaction(self) as transaction:
            self._write_packet(pack_subscribe(topic,
                                              transaction.packet_identifier))

            try:
                reasons = await transaction.wait_until_completed()
            except asyncio.TimeoutError:
                raise TimeoutError('Timeout waiting for SUBACK from the broker.')

            reason = reasons[0]

            if reason != SubackReasonCode.GRANTED_QOS_0:
                raise SubscribeError(reason)

    async def unsubscribe(self, topic):
        """Unsubscribe from given topic.

        Raises :class:`TimeoutError` if the broker does not
        acknowledge the unsubscribe request.

        Raises :class:`UnsubscribeError` if the broker does not accept
        the unsubscribe request.

        >>> await client.unsubscribe('/my/topic')

        """

        with Transaction(self) as transaction:
            self._write_packet(pack_unsubscribe(topic,
                                                transaction.packet_identifier))

            try:
                reasons = await transaction.wait_until_completed()
            except asyncio.TimeoutError:
                raise TimeoutError('Timeout waiting for UNSUBACK from the broker.')

            reason = reasons[0]

            if reason != UnsubackReasonCode.SUCCESS:
                raise UnsubscribeError(reason)

    def publish(self, topic, message, retain=False):
        """Publish given message to given topic with QoS 0.

        Give `retain` as ``True`` to make the message retained.

        >>> client.publish('/my/topic', b'my-message')

        """

        if topic in self._tx_topic_aliases:
            alias = self._tx_topic_aliases[topic]

            if alias in self._registered_broker_topic_aliases:
                topic = ''
        else:
            alias = None

        self._write_packet(pack_publish(topic, message, retain, alias))

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
                    topic = self._rx_topic_aliases[alias]
                except KeyError:
                    LOGGER.debug(
                        'Unknown topic alias %d received from the broker.',
                        alias)
                    return
            elif 0 < alias <= self._rx_topic_alias_maximum:
                self._rx_topic_aliases[alias] = topic
            else:
                LOGGER.debug('Invalid topic alias %d received from the broker.',
                             alias)
                return

        await self.on_message(topic, message)

    def on_suback(self, payload):
        packet_identifier, properties, reasons = unpack_suback(payload)

        if packet_identifier in self.transactions:
            self.transactions[packet_identifier].set_completed(reasons)
        else:
            LOGGER.debug(
                'Discarding unexpected SUBACK packet with identifier %d.',
                packet_identifier)

    def on_unsuback(self, payload):
        packet_identifier, properties, reasons = unpack_unsuback(payload)

        if packet_identifier in self.transactions:
            self.transactions[packet_identifier].set_completed(reasons)
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

        LOGGER.info('Reader task starting...')

        try:
            await self.reader_loop()
        except (Exception, asyncio.CancelledError) as e:
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

        LOGGER.info('Keep alive task starting...')

        try:
            await self.keep_alive_loop()
        except (Exception, asyncio.CancelledError) as e:
            LOGGER.info('Keep alive task stopped by %r.', e)
            await self._close()

    def _write_packet(self, message):
        if LOGGER.isEnabledFor(logging.DEBUG):
            for line in format_packet('Sending', message):
                LOGGER.debug(line)
        elif LOGGER.isEnabledFor(logging.INFO):
            LOGGER.info(format_packet_compact('Sending', message))

        self._writer.write(message)

    async def _read_packet(self):
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
                LOGGER.debug(line)
        elif LOGGER.isEnabledFor(logging.INFO):
            LOGGER.info(format_packet_compact('Received', buf + data))

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
        await self.on_message(None, None)
