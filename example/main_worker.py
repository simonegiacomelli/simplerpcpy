from config import distrib_conf
from simplerpcpy.gmqtt_client import GmqttClient
from simplerpcpy.job_worker import Worker


def main():
    worker = Worker(distrib_conf, GmqttClient(distrib_conf.broker_config).connect())
    print(f'{worker.call_sign} is ready')
    while True:
        job = worker.get_job()
        if job:
            result = eval(job.job)
            job.result = result
            print(job.job, result)
            worker.job_done(job.job_id)


if __name__ == '__main__':
    main()
