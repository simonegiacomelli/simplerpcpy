from typing import Callable

Subscriber = Callable[[str, str], None]


class Client:
    def publish(self, topic: str, payload):
        raise NotImplemented()

    def subscribe(self, topic: str, listener: Subscriber):
        raise NotImplemented()
