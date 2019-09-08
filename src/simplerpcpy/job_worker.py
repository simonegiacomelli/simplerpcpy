import os
import platform
import time
import uuid
from threading import Thread

from simplerpcpy.rpc_consumer import RpcConsumer
from .distributed_conf import MqttConfiguration
from .job_rpc import ManagerRpc, WorkerRpc
from .messaging import Client
from .name_generator import GetRandomName
from .rpc_provider import RpcProvider


class WJob:
    def __init__(self, job_id, job):
        self.job_id = job_id
        self.job = job


class Worker(WorkerRpc):
    def __init__(self, mqtt_conf: MqttConfiguration, client: Client):
        self.mqtt_conf = mqtt_conf
        unique_id = f'{mqtt_conf.manager_queue}/worker/{platform.node()}/{os.getpid()}/{uuid.getnode()}'
        self.worker_endpoint_id = unique_id
        self.call_sign = GetRandomName(0)
        self.job: WJob = None

        self.rpc_provider = RpcProvider(self.worker_endpoint_id, client, self)

        self.manager: ManagerRpc = RpcConsumer(mqtt_conf.manager_queue, client, ManagerRpc()).rpc

        Thread(target=self._signal_status, daemon=True).start()

    def __del__(self):
        self.rpc_provider.unsubscribe()

    def _signal_status(self):
        while True:
            job = self.job
            self.manager.signal_status(self.worker_endpoint_id, self.call_sign, None if not job else job.job_id)
            time.sleep(self.mqtt_conf.presence_interval_seconds)

    def assign_job(self, new_job_id, new_job):
        print('assign_job', new_job_id, new_job)
        job = self.job
        if job:
            print(f'job {job} is locally present')
            if job.job_id == new_job_id:
                print(f'job {new_job_id} was already accepted')
            else:
                pass
                # self.manager.job_rejected(new_job_id)
            return

        self.job = WJob(new_job_id, new_job)
        # self.manager.job_accepted(new_job_id)

    def get_job(self) -> WJob:
        return self.job

    def job_done(self, result):
        self.manager.signal_done(self.worker_endpoint_id, self.job.job_id, result)
        self.job = None
