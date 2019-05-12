import sys
import threading
import asyncio
import re
import time
import bisect

from ..broker import Broker


def _do_broker(args):
    print(f"Starting a broker at '{args.host}:{args.port}'.")
    broker = Broker(args.host, args.port)
    asyncio.run(broker.serve_forever())


def add_subparser(subparsers):
    subparser = subparsers.add_parser('broker',
                                      description='A simple broker.')
    subparser.add_argument('--host',
                           default='localhost',
                           help="Broker host (default: %(default)s).")
    subparser.add_argument('--port',
                           type=int,
                           default=1883,
                           help='Broker port (default: %(default)s).')
    subparser.set_defaults(func=_do_broker)
