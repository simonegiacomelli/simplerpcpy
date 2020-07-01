import random

from config import distrib_conf
from simplerpcpy.gmqtt_client import GmqttClient
from simplerpcpy.job_manager import Manager

client = GmqttClient(distrib_conf.broker_config).connect()
manager = Manager(distrib_conf, client)

# number of jobs to do. You don't need to know beforehand
count = random.randint(100, 200)

for i in range(count):
    # we want the workers to add two numbers
    manager.add({'a': i, 'b': i + 2})

for i in range(count):
    # collect the results. timeout=None will wait until a job is available
    job = manager.get_done(timeout=None)
    print(f'Addition {job.job_payload} = {job.result}; addition kindly done by {job.done_by}')

# if you see errors related to the network, it will recover after the timeouts
