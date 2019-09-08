import time

from gmqtt_client import GmqttClient

c1 = GmqttClient().connect()
c2 = GmqttClient().connect()

c1.subscribe('t1', lambda topic, payload: print(payload))
for i in range(10):
    c2.publish('t1', 'ciao')
    time.sleep(0.2)
