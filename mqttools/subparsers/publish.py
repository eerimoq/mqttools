import asyncio
import time
from humanfriendly import format_timespan

from ..client import Client
from . import try_decode


async def publisher(host, port, client_id, qos, count, size, topic, message):
    client = Client(host, port, client_id)

    await client.start()

    start_time = time.time()

    for number in range(count):
        if message is None:
            message_bytes = str(number).encode('ascii')
            extra = (size - len(message_bytes))

            if extra > 0:
                message_bytes += extra * b'\xa5'
            else:
                message_bytes = message_bytes[:size]
        else:
            message_bytes = message.encode('ascii')

        await client.publish(topic, message_bytes, qos)

    elapsed_time = format_timespan(time.time() - start_time)
    print(f'Published {count} message(s) in {elapsed_time}.')

    await client.stop()


def _do_publish(args):
    asyncio.run(publisher(args.host,
                          args.port,
                          args.client_id,
                          args.qos,
                          args.count,
                          args.size,
                          args.topic,
                          args.message))


def add_subparser(subparsers):
    subparser = subparsers.add_parser('publish',
                                      description='Publish given topic.')
    subparser.add_argument('--host',
                           default='broker.hivemq.com',
                           help='Broker host (default: broker.hivemq.com).')
    subparser.add_argument('--port',
                           type=int,
                           default=1883,
                           help='Broker port (default: 1883).')
    subparser.add_argument('--client-id',
                           help='Client id (default: mqttools-<UUID[0..14]>).')
    subparser.add_argument('--qos',
                           type=int,
                           default=0,
                           help='Quality of service (default: 0).')
    subparser.add_argument(
        '--count',
        type=int,
        default=1,
        help='Number of times to publish the message (default: 1).')
    subparser.add_argument(
        '--size',
        type=int,
        default=50,
        help='Generated message size (default: 50).')
    subparser.add_argument('topic', help='Topic to publish.')
    subparser.add_argument(
        'message',
        nargs='?',
        help='Message to publish (default: <counter>\xa5...')
    subparser.set_defaults(func=_do_publish)
