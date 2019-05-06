import logging
import sys
import argparse

from .version import __version__
from .client import Client
from .client import MalformedPacketError
from .client import ConnectError
from .client import PublishError


def main():
    parser = argparse.ArgumentParser(description='MQTT Tools.')

    parser.add_argument('-d', '--debug', action='store_true')
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

    subscribe.add_subparser(subparsers)
    publish.add_subparser(subparsers)
    monitor.add_subparser(subparsers)

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)

    if args.debug:
        args.func(args)
    else:
        try:
            args.func(args)
        except BaseException as e:
            sys.exit('error: ' + str(e))
