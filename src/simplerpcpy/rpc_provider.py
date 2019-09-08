import json
import traceback

from .messaging import Client


class RpcProvider:
    def __init__(self, topic: str, client: Client, provider):
        self.provider = provider
        self.topic = topic
        self.client = client
        client.subscribe(topic, self._listener)

    def _listener(self, topic, message):
        try:
            o = json.loads(message)
            func = getattr(self.provider, o['method'])
            func(*o['args'], **o.get('kwargs', {}))
        except Exception as ex:
            print(traceback.format_exc(ex))

    def unsubscribe(self):
        self.client.unsubscribe(self.topic)
