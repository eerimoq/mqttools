import asyncio
import time
from humanfriendly import format_timespan

from ..client import Client


def create_message_bytes(message, size, number, fmt):
    if message is None:
        message_bytes = fmt.format(number).encode('ascii')
        extra = (size - len(message_bytes))

        if extra > 0:
            message_bytes += extra * b'\xa5'
        else:
            message_bytes = message_bytes[:size]
    else:
        message_bytes = message.encode('ascii')

    return message_bytes


async def publisher(host,
                    port,
                    client_id,
                    count,
                    size,
                    will_topic,
                    will_message,
                    topic,
                    message):
    if will_message is not None:
        will_message = will_message.encode('utf-8')

    client = Client(host,
                    port,
                    client_id,
                    will_topic=will_topic,
                    will_message=will_message)

    print(f"Connecting to '{host}:{port}'.")
    print()

    await client.start()

    fmt = '{{:0{}}}'.format(len(str(count - 1)))
    start_time = time.time()

    for number in range(count):
        message_bytes = create_message_bytes(message, size, number, fmt)
        client.publish(topic, message_bytes)

    elapsed_time = format_timespan(time.time() - start_time)
    print(f'Published {count} message(s) in {elapsed_time}.')

    await client.stop()


def _do_publish(args):
    asyncio.run(publisher(args.host,
                          args.port,
                          args.client_id,
                          args.count,
                          args.size,
                          args.will_topic,
                          args.will_message,
                          args.topic,
                          args.message))


def add_subparser(subparsers):
    subparser = subparsers.add_parser('publish',
                                      description='Publish given topic.')
    subparser.add_argument('--host',
                           default='broker.hivemq.com',
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
    subparser.add_argument('--will-topic', help='Will topic.')
    subparser.add_argument('--will-message', help='Will message.')
    subparser.add_argument('topic', help='Topic to publish.')
    subparser.add_argument(
        'message',
        nargs='?',
        help='Message to publish (default: <counter>\xa5...')
    subparser.set_defaults(func=_do_publish)
