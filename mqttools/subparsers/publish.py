import ssl
import asyncio
import time
import binascii
from humanfriendly import format_timespan

from ..client import Client
from ..common import Error
from . import to_int


def encode_message(message):
    try:
        return binascii.unhexlify(message)
    except ValueError:
        raise Error(f"Invalid hex string '{message}'.")


def create_message(message, size, number, fmt):
    if message is None:
        message_bytes = fmt.format(number).encode('ascii')
        extra = (size - len(message_bytes))

        if extra > 0:
            message_bytes += extra * b'\xa5'
        else:
            message_bytes = message_bytes[:size]
    else:
        message_bytes = encode_message(message)

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
                    cafile,
                    check_hostname,
                    topic,
                    message):
    if will_message is not None:
        will_message = encode_message(will_message)

    if cafile:
        print(f"CA File:  '{cafile}'")
        print(f"Check hostname: {check_hostname}")

        context = ssl.create_default_context(cafile=cafile)
        context.check_hostname = check_hostname
    else:
        context = None

    client = Client(host,
                    port,
                    client_id,
                    will_topic=will_topic,
                    will_message=will_message,
                    will_retain=will_retain,
                    session_expiry_interval=session_expiry_interval,
                    ssl=context)

    print(f"Connecting to '{host}:{port}'.")
    print()

    await client.start()

    fmt = '{{:0{}}}'.format(len(str(count - 1)))
    start_time = time.time()

    for number in range(count):
        message_bytes = create_message(message, size, number, fmt)
        client.publish(topic, message_bytes, retain=retain)

    elapsed_time = format_timespan(time.time() - start_time)
    print(f'Published {count} message(s) in {elapsed_time}.')

    await client.stop()


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
                          args.cafile,
                          not args.no_check_hostname,
                          args.topic,
                          args.message))


def add_subparser(subparsers):
    subparser = subparsers.add_parser('publish',
                                      description='Publish given topic.')
    subparser.add_argument('--host',
                           default='localhost',
                           help='Broker host (default: %(default)s).')
    subparser.add_argument('--port',
                           type=int,
                           default=1883,
                           help='Broker port (default: %(default)s).')
    subparser.add_argument('--client-id',
                           help='Client id (default: mqttools-<UUID[0..14]>).')
    subparser.add_argument(
        '--count',
        type=int,
        default=1,
        help='Number of times to publish the message (default: %(default)s).')
    subparser.add_argument(
        '--size',
        type=int,
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
    subparser.add_argument(
        '--session-expiry-interval',
        default=0,
        type=to_int,
        help='Session expiry interval in the range 0..0xffffffff (default: %(default)s).')
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
