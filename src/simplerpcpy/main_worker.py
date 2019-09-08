import time

from simplerpcpy.distributed_conf_instances import distrib_conf_test
from simplerpcpy.gmqtt_client import GmqttClient
from simplerpcpy.job_worker import Worker


def main():
    worker = Worker(distrib_conf_test, GmqttClient(distrib_conf_test.broker_config).connect())
    print(f'{worker.call_sign} is ready')
    while True:
        job = worker.get_job()
        if job:
            # time.sleep(1)
            result = eval(job.job)
            job.result = result
            print(job.job, result)
            worker.job_done(job.job_id)
        else:
            time.sleep(1)


if __name__ == '__main__':
    main()
