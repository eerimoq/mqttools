import asyncio
import binascii
import ssl
import time

from argparse_addons import Integer
from humanfriendly import format_timespan

from ..client import Client
from ..client import Message
from ..common import Error


def encode_binary(message):
    try:
        return binascii.unhexlify(message)
    except ValueError:
        raise Error(f"Invalid hex string '{message}'.")


def encode_message(message, kind):
    if kind == 'auto':
        try:
            return binascii.unhexlify(message)
        except ValueError:
            return message.encode()
    elif kind == 'binary':
        return encode_binary(message)
    else:
        return message.encode()


def create_message(message, message_format, size, number, fmt):
    if message is None:
        message_bytes = fmt.format(number).encode('ascii')
        extra = (size - len(message_bytes))

        if extra > 0:
            message_bytes += extra * b'\xa5'
        else:
            message_bytes = message_bytes[:size]
    else:
        message_bytes = encode_message(message, message_format)

    return message_bytes


async def publisher(host,
                    port,
                    client_id,
                    count,
                    size,
                    retain,
                    will_topic,
                    will_message,
                    will_retain,
                    session_expiry_interval,
                    message_format,
                    cafile,
                    check_hostname,
                    topic,
                    message,
                    response_topic,
                    username,
                    password):
    if will_message is not None:
        will_message = encode_message(will_message, message_format)

    if cafile:
        print(f"CA File:  '{cafile}'")
        print(f"Check hostname: {check_hostname}")

        context = ssl.create_default_context(cafile=cafile)
        context.check_hostname = check_hostname
    else:
        context = None

    if password is not None:
        password = password.encode('utf-8')

    print(f"Connecting to '{host}:{port}'.")
    print()

    async with Client(host,
                      port,
                      client_id,
                      will_topic=will_topic,
                      will_message=will_message,
                      will_retain=will_retain,
                      session_expiry_interval=session_expiry_interval,
                      username=username,
                      password=password,
                      ssl=context) as client:
        fmt = '{{:0{}}}'.format(len(str(count - 1)))
        start_time = time.time()

        for number in range(count):
            message_bytes = create_message(message,
                                           message_format,
                                           size,
                                           number,
                                           fmt)
            client.publish(Message(topic,
                                   message_bytes,
                                   retain,
                                   response_topic))

        elapsed_time = format_timespan(time.time() - start_time)
        print(f'Published {count} message(s) in {elapsed_time}.')


def _do_publish(args):
    asyncio.run(publisher(args.host,
                          args.port,
                          args.client_id,
                          args.count,
                          args.size,
                          args.retain,
                          args.will_topic,
                          args.will_message,
                          args.will_retain,
                          args.session_expiry_interval,
                          args.message_format,
                          args.cafile,
                          not args.no_check_hostname,
                          args.topic,
                          args.message,
                          args.response_topic,
                          args.username,
                          args.password))


def add_subparser(subparsers):
    subparser = subparsers.add_parser('publish',
                                      description='Publish given topic.')
    subparser.add_argument('--host',
                           default='localhost',
                           help='Broker host (default: %(default)s).')
    subparser.add_argument('--port',
                           type=Integer(0),
                           default=1883,
                           help='Broker port (default: %(default)s).')
    subparser.add_argument('--client-id',
                           help='Client id (default: mqttools-<UUID[0..14]>).')
    subparser.add_argument(
        '--count',
        type=Integer(0),
        default=1,
        help='Number of times to publish the message (default: %(default)s).')
    subparser.add_argument(
        '--size',
        type=Integer(0),
        default=10,
        help='Generated message size (default: %(default)s).')
    subparser.add_argument('--retain',
                           action='store_true',
                           help='Retain the message.')
    subparser.add_argument('--will-topic', help='Will topic.')
    subparser.add_argument('--will-message', help='Will message.')
    subparser.add_argument('--will-retain',
                           action='store_true',
                           help='Retain the will message.')
    subparser.add_argument('--response-topic',
                           help='Response topic.')
    subparser.add_argument('--username',
                           help='Username.')
    subparser.add_argument('--password',
                           help='Password.')
    subparser.add_argument(
        '--session-expiry-interval',
        type=Integer(0, 0xffffffff),
        default=0,
        help='Session expiry interval in the range 0..0xffffffff (default: %(default)s).')
    subparser.add_argument(
        '--message-format',
        choices=('auto', 'binary', 'text'),
        default='auto',
        help='Message format (default: %(default)s).')
    subparser.add_argument(
        '--cafile',
        default='',
        help='CA file.')
    subparser.add_argument(
        '--no-check-hostname',
        action='store_true',
        help='Do not check certificate hostname.')
    subparser.add_argument('topic', help='Topic to publish.')
    subparser.add_argument(
        'message',
        nargs='?',
        help='Message to publish (default: <counter>\xa5...')
    subparser.set_defaults(func=_do_publish)
