import threading
from datetime import datetime
from typing import Dict, List
import time

from .dist_api import ManagerRpc, WorkerRpc, ManagerFromWorkerRpc
from .job_manager import Job, JobManager
from .distributed_conf import distrib_conf, MqttConfiguration
from .messaging import Client
from .rpc_provider import RpcProvider
from simplerpcpy.rpc_consumer import RpcConsumer


class Manager(ManagerRpc, JobManager):
    def __init__(self, mqtt_conf: MqttConfiguration, client: Client):
        self.mqtt_conf = mqtt_conf
        self.client = client
        self.rpc_provider = RpcProvider(mqtt_conf.manager_queue, client, self)
        self.workers: Dict[str, WorkerInfo] = {}

        self.todo: List[Job] = []
        self.done: List[Job] = []

        self.ready = threading.Event()

        threading.Thread(target=self._book_keeping, daemon=True).start()

    def add(self, job: Job):
        self.todo.append(job)

    def get_done(self, timeout=0):
        if timeout > 0 and len(self.done) == 0:
            self.ready.clear()
            self.ready.wait(timeout)
        if len(self.done) > 0:
            return self.done.pop(0)
        return None

    def signal_presence(self, worker_endpoint_id, manager_endpoint_id, call_sign, timestamp):
        worker = self.workers.get(worker_endpoint_id, None)
        if not worker:
            worker = WorkerInfo(self)
            worker.queue_id = worker_endpoint_id
            worker.manager_endpoint_id = manager_endpoint_id
            worker.call_sign = call_sign
            # get RpcConsumer for worker
            worker.worker_rpc = WorkerRpc()
            RpcConsumer(worker_endpoint_id, self.client, worker.worker_rpc)
            RpcProvider(worker.manager_endpoint_id, self.client, worker)
            print(f'new worker entered the pool {worker}')

        worker.signal_count += 1
        worker.last_signal_time = datetime.now()
        worker.remote_timestamp = timestamp
        self.workers[worker_endpoint_id] = worker

    def _book_keeping(self):

        while True:
            self._prune_workers()
            self._assign_jobs()
            time.sleep(self.mqtt_conf.manager_book_keepint_interval)

    def _prune_workers(self):
        now = datetime.now()

        for id, worker in list(self.workers.items()):
            delta: datetime.timedelta = now - worker.last_signal_time
            if delta.seconds > distrib_conf.prune_timeout:
                self.workers.pop(id)
                print(f'pruning unresponsive worker {worker}')
                if worker.job and not worker.job.done:
                    print(f'  restoring job {worker.job.job_id} ')
                    self.todo.append(worker.job)

    def _assign_jobs(self):
        while len(self.todo) > 0:
            worker = self._get_free_worker()
            if not worker:
                # print('jobs todo but no worker free')
                return
            worker.free = False
            worker.job = self.todo.pop(0)
            worker.worker_rpc.assign_job(worker.job.job_id, worker.job.job)

    def _get_free_worker(self):
        return next((w for queue, w in self.workers.items() if w.free), None)

    def _signal_job_done(self, worker: 'WorkerInfo'):
        # print('job is done', worker.job.job_id)
        self.done.append(worker.job)
        self.ready.set()
        worker.job = None
        worker.accepted = False
        worker.free = True
        self._assign_jobs()


class WorkerInfo(ManagerFromWorkerRpc):
    queue_id = ''
    call_sign = ''
    last_signal_time = None
    remote_timestamp = ''
    signal_count = 0
    worker_rpc: WorkerRpc = None
    free = True
    accepted = False
    job: Job = None

    def __init__(self, manager: Manager):
        self.manager = manager

    def __repr__(self):
        return f'{self.call_sign} ({"free" if self.free else "busy"} {self.queue_id})'

    def job_accepted(self, job_id):
        if self.free:
            print(f'{self} was free!!')
            return
        if self.job.job_id != job_id:
            print(f'{self} has a different job assigned; manager={self.job.job_id} worker={job_id}')
            return
        # print(self.queue_id, 'job accepted', self.job.job_id)
        self.accepted = True

    def job_rejected(self, job_id):
        print('job rejected TODO')

    def job_done(self, job_id, result):
        if not self.job:
            print(f'{self} has no job assigned! ignoring job_done {job_id}')
            return
        if self.job.job_id != job_id:
            print(f'{self} has a different job assigned; manager={self.job.job_id} worker={job_id}')
        self.job.result = result
        self.job.done = True
        self.manager._signal_job_done(self)
