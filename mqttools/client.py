import logging
import asyncio
import enum
import uuid
import bitstruct

from .common import ControlPacketType
from .common import ConnectReasonCode
from .common import PropertyIds
from .common import SubackReasonCode
from .common import UnsubackReasonCode
from .common import DisconnectReasonCode
from .common import QoS
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


LOGGER = logging.getLogger(__name__)


class SessionResumeError(Error):
    pass


class ReasonError(Error):

    def __init__(self, reason):
        super().__init__()
        self.reason = reason

    def __str__(self):
        if isinstance(self.reason, enum.Enum):
            return f'{self.reason.name}({self.reason.value})'
        else:
            return f'UNKNOWN({self.reason})'


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
        procedure. This method must be called before any
        :meth:`publish()` or :meth:`subscribe()` calls. Call
        :meth:`stop()` to close the connection.

        If `resume_session` is ``True``, the client tries to resume
        the last session in the broker. A :class:`SessionResumeError`
        exception is raised if the resume fails, and a new session has
        been created instead.

        Raises :class:`TimeoutError` if the broker does not
        acknowledge the connect request.

        Raises :class:`ConnectError` if the broker does not accept the
        connect request.

        >>> await client.start()

        Trying to resume a session.

        >>> try:
        ...     await client.start(resume_session=True)
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
        connection. Call :meth:`start()` after :meth:`stop()` to
        reconnect to the broker.

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
            raise TimeoutError('Timeout waiting for CONNACK from the broker.')

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
                        'Unknown topic alias %d received from the broker.',
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
            for line in format_packet('Sending', message):
                LOGGER.debug(line)

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
            for line in format_packet('Received', buf + data):
                LOGGER.debug(line)

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
