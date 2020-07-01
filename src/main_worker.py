from config import distrib_conf
from simplerpcpy.gmqtt_client import GmqttClient
from simplerpcpy.job_worker import Worker

worker = Worker(distrib_conf, GmqttClient(distrib_conf.broker_config).connect())
print(f'{worker.call_sign} is ready')
while True:
    job = worker.get_job()
    if job:
        a = job.job['a']
        b = job.job['b']
        job.result = a + b
        print(f'{worker.call_sign} did {a}+{b}={job.result}')
        worker.job_done(job.job_id)
