import time
from collections import deque
from math import ceil


class WindowRate:
    def __init__(self, window_in_seconds):
        self.window_in_seconds = window_in_seconds
        self.q = deque()

    def spin(self):
        self.q.append(time.time())
        self._prune()

    def _prune(self):
        t = time.time()
        while len(self.q) > 0 and t - self.q[0] > self.window_in_seconds:
            self.q.popleft()

    def rate(self):
        self._prune()
        return len(self.q)


def main():
    wr = WindowRate(10)
    while True:
        wr.spin()
        time.sleep(0.1)
        print('rate', wr.rate())


if __name__ == '__main__':
    main()
