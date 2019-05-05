import asyncio

from ..client import Client
from . import try_decode


async def publisher(host, port, client_id, topic, message, qos):
    client = Client(host, port, client_id)

    await client.start()

    print(f'Topic:   {topic}')
    print(f'Message: {try_decode(message)}')
    print(f'QoS:     {qos}')

    await client.publish(topic, message, qos)
    await client.stop()


def _do_publish(args):
    asyncio.run(publisher(args.host,
                          args.port,
                          args.client_id,
                          args.topic,
                          args.message.encode('ascii'),
                          args.qos))


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
    subparser.add_argument('topic', help='Topic to publish.')
    subparser.add_argument('message', help='Message to publish.')
    subparser.set_defaults(func=_do_publish)
