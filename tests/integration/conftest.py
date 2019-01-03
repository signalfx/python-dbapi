# Copyright (C) 2018 SignalFx, Inc. All rights reserved.
from random import choice, random, randint
import string


class DBAPITest(object):

    def fmt_time(self, ts):
        return ts.strftime('%Y-%m-%d %H:%M:%S')

    _strings = set()
    _ints = set()
    _floats = set()

    def random_string(self):
        while True:
            s = ''.join(choice(string.ascii_lowercase) for _ in range(10))
            if s not in self._strings:
                self._strings.add(s)
                return s

    def random_int(self):
        while True:
            i = randint(0, 100000)
            if i not in self._ints:
                self._ints.add(i)
                return i

    def random_float(self):
        while True:
            i = random() * 100000
            if i not in self._floats:
                self._floats.add(i)
                return i
