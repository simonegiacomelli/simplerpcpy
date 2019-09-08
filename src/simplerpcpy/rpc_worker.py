import os
import platform
import time
import uuid
from datetime import datetime
from threading import Thread

from .dist_api import ManagerRpc, WorkerRpc, ManagerFromWorkerRpc
from .distributed_conf import MqttConfiguration
from .messaging import Client
from .name_generator import GetRandomName
from .rpc_provider import RpcProvider
from simplerpcpy.rpc_consumer import RpcConsumer


class WorkerJob:
    result = None

    def __init__(self, job_id, job):
        self.job_id = job_id
        self.job = job


class Worker(WorkerRpc):
    def __init__(self, mqtt_conf: MqttConfiguration, client: Client):
        self.mqtt_conf = mqtt_conf
        unique_id = mqtt_conf.manager_queue + '/worker/' + platform.node() + '/' + str(os.getpid()) + '/' + str(
            uuid.getnode())
        self.worker_endpoint_id = unique_id
        self.manager_endpoint_id = self.worker_endpoint_id + '/listener'
        self.call_sign = GetRandomName(0)
        self.job: WorkerJob = None
        self.job_id = None

        self.rpc_provider = RpcProvider(self.worker_endpoint_id, client, self)

        self.manager_rpc = ManagerRpc()
        RpcConsumer(mqtt_conf.manager_queue, client, self.manager_rpc)
        self.manager_wrpc = ManagerFromWorkerRpc()
        RpcConsumer(self.manager_endpoint_id, client, self.manager_wrpc)

        Thread(target=self._signal_presence, daemon=True).start()

    def _signal_presence(self):
        while True:
            self.manager_rpc.signal_presence(self.worker_endpoint_id, self.manager_endpoint_id, self.call_sign,
                                             str(datetime.now()))
            time.sleep(self.mqtt_conf.presence_interval_seconds)

    def assign_job(self, job_id, job):
        print('jobs', job_id, job)
        if self.job_id:
            print(f'job {self.job_id} is locally present')
            if self.job_id == job_id:
                print(f'job {job_id} was already accepted')
            else:
                self.manager_wrpc.job_rejected(job_id)
            return

        self.job = WorkerJob(job_id, job)
        self.manager_wrpc.job_accepted(job_id)

    def get_job(self) -> WorkerJob:
        return self.job

    def job_done(self):
        self.manager_wrpc.job_done(self.job.job_id, self.job.result)
        self.job = None
