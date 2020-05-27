import logging
import sys
import argparse

from .version import __version__
from .client import Client
from .client import ConnectError
from .client import SessionResumeError
from .client import SubscribeError
from .client import UnsubscribeError
from .broker import Broker
from .broker import BrokerThread
from .common import MalformedPacketError
from .common import TimeoutError
from .common import SubackReasonCode
from .common import UnsubackReasonCode


def main():
    parser = argparse.ArgumentParser(description='MQTT Tools.')

    parser.add_argument('-d', '--debug', action='store_true')
    parser.add_argument('-l', '--log-level',
                        default='error',
                        choices=[
                            'debug', 'info', 'warning', 'error', 'critical'
                        ],
                        help='Set the logging level (default: %(default)s).')
    parser.add_argument('--version',
                        action='version',
                        version=__version__,
                        help='Print version information and exit.')

    # Workaround to make the subparser required in Python 3.
    subparsers = parser.add_subparsers(title='subcommands',
                                       dest='subcommand')
    subparsers.required = True

    # Import when used for less dependencies. For example, curses is
    # not part of all Python builds.
    from .subparsers import subscribe
    from .subparsers import publish
    from .subparsers import monitor
    from .subparsers import broker

    subscribe.add_subparser(subparsers)
    publish.add_subparser(subparsers)
    monitor.add_subparser(subparsers)
    broker.add_subparser(subparsers)

    args = parser.parse_args()

    level = logging.getLevelName(args.log_level.upper())
    logging.basicConfig(level=level, format='%(asctime)s %(message)s')

    if args.debug:
        args.func(args)
    else:
        try:
            args.func(args)
        except BaseException as e:
            sys.exit('error: ' + str(e))
