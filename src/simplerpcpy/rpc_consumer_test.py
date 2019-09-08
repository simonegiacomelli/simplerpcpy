import json
import unittest

from simplerpcpy.messaging import Client
from simplerpcpy.rpc_consumer import RpcConsumer


class ClientMock(Client):
    messages = []

    def publish(self, topic, payload):
        self.messages.append((topic, json.loads(payload)))


class Proxy:
    def hello1(self):
        print('hello1')

    def hello2(self, name):
        print('hello2', name)


client = ClientMock()


class ConsumerTest(unittest.TestCase):
    def test1(self):
        target = RpcConsumer('topic1', client, Proxy())
        target.rpc.hello1()
        target.rpc.hello2('Foo')
        self.assertEqual(len(client.messages), 2)
        h1 = client.messages[0][1]
        h2 = client.messages[1][1]
        self.assertEqual(h1['method'], 'hello1')
        self.assertEqual(h2['method'], 'hello2')
        self.assertEqual(h2['args'], ['Foo'])
