import json

from simplerpcpy.messaging import Client


# class _Instrument
class RpcConsumer:

    def __init__(self, topic: str, client: Client, proxy):
        self.topic = topic
        self.client = client
        redirect_all_calls(proxy, self._handle)

    def _handle(self, name, *args, **kwargs):
        o = {'method': name, 'args': args}
        if len(kwargs) > 0:
            o['kwargs'] = kwargs
        payload = json.dumps(o)
        self.client.publish(self.topic, payload)


def redirect_all_calls(proxy, handle):
    filt1 = (name for name in dir(proxy))
    functions = (f for f in (getattr(proxy, name) for name in filt1
                             if not (name.startswith('__') and name.endswith('__')))
                 if callable(f))
    for func in functions:
        name = func.__name__

        def scope(name):
            def newfunc(*args, **kwargs):
                handle(name, *args, **kwargs)

            newfunc.__name__ = name
            return newfunc

        setattr(proxy, name, scope(name))
