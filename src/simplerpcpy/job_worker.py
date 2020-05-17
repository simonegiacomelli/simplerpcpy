import os
import platform
import threading
import time
import uuid
from threading import Thread

from simplerpcpy.job_abc import WorkerAbc, WJob
from simplerpcpy.rpc_consumer import RpcConsumer
from .distributed_conf import MqttConfiguration
from .job_rpc import ManagerRpc, WorkerRpc
from .messaging import Client
from .name_generator import GetRandomName
from .rpc_provider import RpcProvider


class Worker(WorkerRpc, WorkerAbc):
    def __init__(self, mqtt_conf: MqttConfiguration, client: Client):
        self.mqtt_conf = mqtt_conf
        unique_id = f'{mqtt_conf.manager_queue}/worker/{platform.node()}/{os.getpid()}/{uuid.getnode()}'
        self.worker_endpoint_id = unique_id
        self.call_sign = GetRandomName(0)
        self.job: WJob = None
        self.signal_done = threading.Event()
        self.signal_todo = threading.Event()
        self.rpc_provider = RpcProvider(self.worker_endpoint_id, client, self)

        self.manager: ManagerRpc = RpcConsumer(mqtt_conf.manager_queue, client, ManagerRpc()).rpc

        Thread(target=self._signal_status, daemon=True).start()

    def __del__(self):
        self.rpc_provider.unsubscribe()

    def _signal_status(self):
        while True:
            job = self.job
            job_id = None
            job_done = False
            job_result = None
            if job:
                job_id = job.job_id
                job_done = job.done
                job_result = job.result
            self.manager.signal_status(self.worker_endpoint_id, self.call_sign, job_id, job_done, job_result)
            self.signal_done.clear()
            self.signal_done.wait(self.mqtt_conf.presence_interval_seconds)

    def reset_job(self, job_id):
        if self.job and self.job.job_id == job_id:
            self.job = None

    def assign_job(self, new_job_id, new_job):
        print('assign_job', new_job_id, new_job)
        job = self.job
        if job and not job.done:
            print(f'job {job} is locally present')
            if job.job_id == new_job_id:
                print(f'job {new_job_id} was already accepted')
            else:
                print(f'job {new_job_id} is rejected')
                # self.manager.job_rejected(new_job_id)
            return

        self.job = WJob(new_job_id, new_job)
        self.signal_todo.set()
        # self.manager.job_accepted(new_job_id)

    def get_job(self, timeout=0) -> WJob:
        job = self._get_job()
        if not job and timeout > 0:
            self.signal_done.clear()
            self.signal_todo.wait(float(timeout))
        job = self._get_job()
        return job

    def _get_job(self):
        job = self.job
        if job and job.done:
            return None
        else:
            return job

    def job_done(self, job_id):
        if self.job and self.job.job_id == job_id:
            self.job.done = True
            self.signal_done.set()
