from __future__ import print_function
import re
import time
import curses
import bisect
from queue import Queue
from queue import Empty as QueueEmpty


class QuitError(Exception):
    pass


class Monitor(object):

    def __init__(self, stdscr, args):
        self._stdscr = stdscr
        self._single_line = args.single_line
        self._filtered_sorted_message_names = []
        self._filter = ''
        self._compiled_filter = None
        self._formatted_messages = {}
        self._playing = True
        self._modified = True
        self._queue = Queue()
        self._nrows, self._ncols = stdscr.getmaxyx()
        self._received = 0
        self._discarded = 0
        self._basetime = None

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
        self.draw_stats(0)
        self.draw_title(1)

        row = 2

        for name in self._filtered_sorted_message_names:
            for line in self._formatted_messages[name]:
                self.addstr(row, 0, line)
                row += 1

                if row > self._nrows - 2:
                    break

        self.draw_menu(self._nrows - 1)

        # Refresh the screen.
        self._stdscr.refresh()

    def draw_stats(self, row):
        self.addstr(row, 0, 'Received: {}, Discarded: {}, Errors: 0'.format(
            self._received,
            self._discarded))

    def draw_title(self, row):
        self.addstr_color(row,
                          0,
                          self.stretch('   TIMESTAMP  MESSAGE'),
                          curses.color_pair(1))

    def draw_menu(self, row):
        text = 'q: Quit'

        self.addstr_color(row,
                          0,
                          self.stretch(text),
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

    def compile_filter(self):
        try:
            self._compiled_filter = re.compile(self._filter)
        except:
            self._compiled_filter = None

    def try_update_message(self):
        message = self._queue.get_nowait()
        frame_id = message.arbitration_id
        data = message.data
        timestamp = message.timestamp

        if self._basetime is None:
            self._basetime = timestamp

        timestamp -= self._basetime
        self._received += 1

        try:
            message = self._dbase.get_message_by_frame_id(frame_id)
        except KeyError:
            self._discarded += 1
            return

        if len(data) != message.length:
            self._discarded += 1
            return

        name = message.name

        if message.is_multiplexed():
            name = format_multiplexed_name(message, data, True)

        if self._single_line:
            formatted = format_message(message, data, True, True)
            self._formatted_messages[name] = [
                '{:12.3f} {}'.format(timestamp, formatted)
            ]
        else:
            formatted = format_message(message, data, True, False)
            lines = formatted.splitlines()
            formatted = ['{:12.3f}  {}'.format(timestamp, lines[1])]
            formatted += [14 * ' ' + line for line in lines[2:]]
            self._formatted_messages[name] = formatted

        if name not in self._filtered_sorted_message_names:
            self.insort_filtered(name)

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

    def insort_filtered(self, name):
        if self._compiled_filter is None or self._compiled_filter.search(name):
            bisect.insort(self._filtered_sorted_message_names,
                          name)


def do_monitor(args):
    def monitor(stdscr):
        Monitor(stdscr, args).run()

    try:
        curses.wrapper(monitor)
    except KeyboardInterrupt:
        pass
