import json
from typing import TypeVar, Generic

from simplerpcpy.messaging import Client

T = TypeVar("T")


# class _Instrument
class RpcConsumer(Generic[T]):

    def __init__(self, topic: str, client: Client, target: T):
        self.rpc: T = target
        self.topic = topic
        self.client = client
        redirect_all_calls(target, self._handle)

    def _handle(self, name, *args, **kwargs):
        o = {'method': name, 'args': args}
        if len(kwargs) > 0:
            o['kwargs'] = kwargs
        payload = json.dumps(o)
        self.client.publish(self.topic, payload)


def redirect_all_calls(target, handle):
    filt1 = (name for name in dir(target))
    functions = (f for f in (getattr(target, name) for name in filt1
                             if not (name.startswith('__') and name.endswith('__')))
                 if callable(f))
    for func in functions:
        name = func.__name__

        def scope(name):
            def newfunc(*args, **kwargs):
                handle(name, *args, **kwargs)

            newfunc.__name__ = name
            return newfunc

        setattr(target, name, scope(name))
