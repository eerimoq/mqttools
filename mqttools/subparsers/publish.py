import asyncio
import time
from humanfriendly import format_timespan

from ..client import Client


class Counter(object):

    def __init__(self, count):
        self._lock = asyncio.Lock()
        self._number = 0
        self._count = count

    async def get(self):
        number = None

        async with self._lock:
            if self._number < self._count:
                number = self._number
                self._number += 1

        return number


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


async def worker(client, counter, qos, size, topic, message, fmt):
    while True:
        number = await counter.get()

        if number is None:
            break

        message_bytes = create_message_bytes(message, size, number, fmt)
        await client.publish(topic, message_bytes, qos)


async def publisher(host, port, client_id, qos, count, size, topic, message):
    client = Client(host, port, client_id)

    await client.start()

    fmt = '{{:0{}}}'.format(len(str(count - 1)))
    start_time = time.time()

    if qos == 0:
        number_of_concurrent_tasks = 1

        for number in range(count):
            message_bytes = create_message_bytes(message, size, number, fmt)
            await client.publish(topic, message_bytes, qos)
    else:
        counter = Counter(count)
        number_of_concurrent_tasks = client.broker_receive_maximum
        await asyncio.gather(*[
            asyncio.create_task(
                worker(client, counter, qos, size, topic, message, fmt))
            for _ in range(number_of_concurrent_tasks)
        ])

    elapsed_time = format_timespan(time.time() - start_time)
    print(f'Published {count} message(s) in {elapsed_time} from '
          f'{number_of_concurrent_tasks} concurrent task(s).')

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
        default=10,
        help='Generated message size (default: 10).')
    subparser.add_argument('topic', help='Topic to publish.')
    subparser.add_argument(
        'message',
        nargs='?',
        help='Message to publish (default: <counter>\xa5...')
    subparser.set_defaults(func=_do_publish)
