import asyncio
import ssl

from ..broker import Broker


def _do_broker(args):
    print(f"Starting a broker at '{args.host}:{args.port}'.")

    if all([args.cafile, args.certfile, args.keyfile]):
        print(f"Certfile: '{args.certfile}'")
        print(f"Keyfile:  '{args.keyfile}'")
        print(f"CA File:  '{args.cafile}'")
        print(f"Check hostname: {not args.no_check_hostname}")

        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH,
                                             cafile=args.cafile)
        context.check_hostname = not args.no_check_hostname
        context.load_cert_chain(certfile=args.certfile, keyfile=args.keyfile)
    else:
        context = None

    broker = Broker(args.host, args.port, args.secure_port, secure_ssl=context)
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
    subparser.add_argument('--secure-port',
                           type=int,
                           default=8883,
                           help='Secure broker port (default: %(default)s).')
    subparser.add_argument(
        '--cafile',
        default='',
        help='MQTT broker CA file.')
    subparser.add_argument(
        '--certfile',
        default='',
        help='MQTT broker certificate file.')
    subparser.add_argument(
        '--keyfile',
        default='',
        help='MQTT broker key file.')
    subparser.add_argument(
        '--no-check-hostname',
        action='store_true',
        help='Do not check certificate hostname.')
    subparser.set_defaults(func=_do_broker)
