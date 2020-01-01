import ssl
import sys
import threading
import asyncio
import time
import curses
import bisect
from queue import Queue
from queue import Empty as QueueEmpty

from ..client import Client
from ..common import hexlify
from . import to_int


class QuitError(Exception):
    pass


class ClientThread(threading.Thread):

    def __init__(self, queue, args):
        super().__init__()
        self._queue = queue
        self._host = args.host
        self._port = args.port
        self._cafile = args.cafile
        self._check_hostname = not args.no_check_hostname
        self._client_id = args.client_id
        self._keep_alive_s = args.keep_alive
        self._session_expiry_interval = args.session_expiry_interval
        self._topics = args.subscribe

    async def main(self):
        if self._cafile:
            print(f"CA File:  '{self._cafile}'")
            print(f"Check hostname: {self._check_hostname}")

            context = ssl.create_default_context(cafile=self._cafile)
            context.check_hostname = self._check_hostname
        else:
            context = None

        client = Client(self._host,
                        self._port,
                        self._client_id,
                        keep_alive_s=self._keep_alive_s,
                        session_expiry_interval=self._session_expiry_interval,
                        subscriptions=self._topics,
                        topic_alias_maximum=10,
                        ssl=context)

        while True:
            await client.start()

            while True:
                topic, message = await client.messages.get()

                if topic is None:
                    break

                self._queue.put((time.strftime('%H:%M:%S'), topic, message))

            await client.stop()

    def run(self):
        asyncio.run(self.main())


class Monitor(object):

    def __init__(self, stdscr, args):
        self._stdscr = stdscr
        self._sorted_topics = []
        self._formatted_messages = {}
        self._playing = True
        self._modified = True
        self._queue = Queue()
        self._nrows, self._ncols = stdscr.getmaxyx()
        self._client_thread = ClientThread(self._queue, args)
        self._client_thread.daemon = True
        self._client_thread.start()

        stdscr.nodelay(True)
        curses.use_default_colors()
        curses.curs_set(False)
        curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_GREEN)
        curses.init_pair(2, curses.COLOR_BLACK, curses.COLOR_CYAN)

    def run(self):
        while True:
            try:
                self.tick()
            except QuitError:
                break

            time.sleep(0.05)

    def tick(self):
        modified = self.update()

        if modified:
            self.redraw()

        self.process_user_input()

    def redraw(self):
        # Clear the screen.
        self._stdscr.clear()

        # Draw everything.
        self.draw_title(0)

        row = 1

        for topic in self._sorted_topics:
            for line in self._formatted_messages[topic]:
                self.addstr(row, 0, line)
                row += 1

                if row > self._nrows - 2:
                    break

        self.draw_menu(self._nrows - 1)

        # Refresh the screen.
        self._stdscr.refresh()

    def draw_title(self, row):
        self.addstr_color(row,
                          0,
                          self.stretch('TIMESTAMP  TOPIC & MESSAGE'),
                          curses.color_pair(1))

    def draw_menu(self, row):
        self.addstr_color(row,
                          0,
                          self.stretch('q: Quit, p: Play/Pause'),
                          curses.color_pair(2))

    def addstr(self, row, col, text):
        try:
            self._stdscr.addstr(row, col, text)
        except curses.error:
            pass

    def addstr_color(self, row, col, text, color):
        try:
            self._stdscr.addstr(row, col, text, color)
        except curses.error:
            pass

    def stretch(self, text):
        return text + ' ' * (self._ncols - len(text))

    def process_user_input(self):
        try:
            key = self._stdscr.getkey()
        except curses.error:
            return

        self.process_user_input_menu(key)

    def process_user_input_menu(self, key):
        if key == 'q':
            raise QuitError()
        elif key == 'p':
            self._playing = not self._playing

    def try_update_message(self):
        timestamp, topic, message = self._queue.get_nowait()

        if topic is None:
            sys.exit('Broker connection lost!')

        lines = []
        row_length = max(1, self._ncols - 12)
        message = hexlify(message)

        for i in range(0, len(message), row_length):
            lines.append(message[i:i + row_length])

        formatted = [' {}  {}'.format(timestamp, topic)]
        formatted += [11 * ' ' + line for line in lines]
        self._formatted_messages[topic] = formatted

        if topic not in self._sorted_topics:
            self.insort(topic)

    def update_messages(self):
        modified = False

        try:
            while True:
                self.try_update_message()
                modified = True
        except QueueEmpty:
            pass

        return modified

    def update(self):
        if self._playing:
            modified = self.update_messages()
        else:
            modified = False

        if self._modified:
            self._modified = False
            modified = True

        if curses.is_term_resized(self._nrows, self._ncols):
            self._nrows, self._ncols = self._stdscr.getmaxyx()
            modified = True

        return modified

    def insort(self, topic):
        bisect.insort(self._sorted_topics, topic)


def _do_monitor(args):
    def monitor(stdscr):
        Monitor(stdscr, args).run()

    try:
        curses.wrapper(monitor)
    except KeyboardInterrupt:
        pass


def add_subparser(subparsers):
    subparser = subparsers.add_parser('monitor',
                                      description='Monitor given topics.')
    subparser.add_argument('--host',
                           default='localhost',
                           help='Broker host (default: %(default)s).')
    subparser.add_argument('--port',
                           type=int,
                           default=1883,
                           help='Broker port (default: %(default)s).')
    subparser.add_argument('--client-id',
                           help='Client id (default: mqttools-<UUID[0..14]>).')
    subparser.add_argument('--keep-alive',
                           type=int,
                           default=0,
                           help=('Keep alive time in seconds (default: '
                                 '%(default)s). Give as 0 to disable keep '
                                 'alive.'))
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
    subparser.add_argument(
        'subscribe',
        nargs='*',
        help='Subscribe to given topic(s) <topic>.')
    subparser.set_defaults(func=_do_monitor)
