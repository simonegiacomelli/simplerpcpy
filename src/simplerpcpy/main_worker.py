import time

from distributed_conf import distrib_conf_test
from gmqtt_client import GmqttClient
from rpc_worker import Worker


def main():
    worker = Worker(distrib_conf_test, GmqttClient(distrib_conf_test.broker_config).connect())
    print(f'{worker.call_sign} is ready')
    while True:
        job = worker.get_job()
        if job:
            # time.sleep(1)
            result = eval(job.job)
            print(job.job, result)
            job.result = result
            worker.job_done()
        else:
            time.sleep(1)


if __name__ == '__main__':
    main()
