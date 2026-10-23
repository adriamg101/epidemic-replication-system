from contextlib import contextmanager
from collections import deque
import threading


class ReadWriteLock:
    def __init__(self):
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        self.queue = deque()
        self.active_readers = 0
        self.active_writer = False

    def _wait_turn(self, my_turn):
        with self.condition:
            while self.queue[0] is not my_turn or self.active_writer:
                self.condition.wait()

    def r_acquire(self):
        my_turn = object()
        with self.lock:
            self.queue.append(my_turn)
        self._wait_turn(my_turn)
        with self.lock:
            self.active_readers += 1
            self.queue.popleft()
            self.condition.notify_all()

    def r_release(self):
        with self.lock:
            self.active_readers -= 1
            if self.active_readers == 0:
                self.condition.notify_all()

    def w_acquire(self):
        my_turn = object()
        with self.lock:
            self.queue.append(my_turn)
        self._wait_turn(my_turn)
        with self.lock:
            self.active_writer = True
            self.queue.popleft()

    def w_release(self):
        with self.lock:
            self.active_writer = False
            self.condition.notify_all()

    @contextmanager
    def r_locked(self):
        self.r_acquire()
        try:
            yield
        finally:
            self.r_release()

    @contextmanager
    def w_locked(self):
        self.w_acquire()
        try:
            yield
        finally:
            self.w_release()
