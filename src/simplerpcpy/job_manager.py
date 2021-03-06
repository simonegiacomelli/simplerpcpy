import threading
from collections import OrderedDict
from datetime import datetime
from typing import Dict

from simplerpcpy.rpc_consumer import RpcConsumer
from .distributed_conf import MqttConfiguration
from .job_abc import MJobAbc, ManagerAbc
from .job_rpc import ManagerRpc, WorkerRpc
from .messaging import Client
from .rpc_provider import RpcProvider


class MJob(MJobAbc):
    worker: 'MWorker' = None

    def restore(self):
        if self.worker:
            self.worker.job = None
        self.worker = None

    def assign(self, worker: 'MWorker'):
        assert not self.worker, self.worker
        assert not worker.job, worker.job
        worker.job = self
        self.worker = worker

    @property
    def assignable(self):
        return not self.done and not self.worker

    def __repr__(self):
        return f'{self.job_id} done={self.done}'


class MWorker:
    queue_id = ''
    call_sign = ''
    last_signal_time = None
    worker_rpc: WorkerRpc = None
    job: MJob = None
    working_on_unknown = False

    @property
    def free(self):
        return not self.job and not self.working_on_unknown

    def __repr__(self):
        return f'{self.call_sign} job={self.job}'


class Manager(ManagerRpc, ManagerAbc):
    def __init__(self, mqtt_conf: MqttConfiguration, client: Client):
        self.mqtt_conf = mqtt_conf
        self.client = client
        self.rpc_provider = RpcProvider(mqtt_conf.manager_queue, client, self)

        self.jobs: OrderedDict[str, MJob] = OrderedDict()
        self.workers: Dict[str, MWorker] = {}

        self.ready = threading.Event()
        self.do_book_keeping = threading.Event()
        self.lock = threading.Lock()

        threading.Thread(target=self._book_keeping, daemon=True).start()

    def __del__(self):
        self.rpc_provider.unsubscribe()

    def add(self, job) -> MJobAbc:
        assert not isinstance(job, MJobAbc), type(job)
        mjob = MJob(job)

        def _add():
            self.jobs[mjob.job_id] = mjob

        self._lock(_add)
        return mjob

    def get_done(self, timeout=0.5) -> MJobAbc:
        def safe():
            job = next((j for j in self.jobs.values() if j.done), None)
            if job:
                self.jobs.pop(job.job_id)
            return job

        job = None
        while job is None:
            job = self._lock(safe)
            if timeout == 0:
                break
            if not job:
                self.ready.clear()
                self.ready.wait(timeout)
        return job

    def _lock(self, cmd):
        self.lock.acquire()
        try:
            return cmd()
        finally:
            self.lock.release()

    def signal_status(self, worker_endpoint_id, call_sign, job_id, job_done, job_result):
        self._lock(lambda: self._signal_status(worker_endpoint_id, call_sign, job_id, job_done, job_result))
        self.do_book_keeping.set()

    def _signal_status(self, worker_endpoint_id: str, call_sign: str, job_id: str, job_done, job_result):
        worker = self._get_worker(worker_endpoint_id, call_sign)

        worker.last_signal_time = datetime.now()

        job: MJob = None if not job_id else self.jobs.get(job_id, None)

        # if we have a job_id but no corresponding MJob
        worker.working_on_unknown = job_id and not job and not job_done

        if worker.job and worker.job != job:  # assigned is different from doing. needs to restore job
            worker.job.restore()

        if not worker.job and job and not job.worker:  # reconnecting job to worker
            job.assign(worker)

        if worker.job and worker.job == job and not job.done and job_done:
            job.result = job_result
            job.done = True
            job.done_by = worker.call_sign
            job.worker = None
            worker.job = None
            self.ready.set()
            worker.worker_rpc.reset_job(job_id)

    def _get_worker(self, worker_endpoint_id, call_sign=None):
        worker = self.workers.get(worker_endpoint_id, None)
        if not worker:
            worker = MWorker()
            if call_sign:
                worker.call_sign = call_sign
            worker.queue_id = worker_endpoint_id
            worker.worker_rpc = RpcConsumer(worker_endpoint_id, self.client, WorkerRpc()).rpc
            self.workers[worker_endpoint_id] = worker
            print(f'new worker entered the pool {worker}')
        return worker

    def _book_keeping(self):

        while True:
            self._lock(lambda: self._prune_workers())
            self._lock(lambda: self._assign_jobs())
            self.do_book_keeping.clear()
            self.do_book_keeping.wait(self.mqtt_conf.manager_book_keeping_interval)

    def _prune_workers(self):
        now = datetime.now()

        for w_id, worker in list(self.workers.items()):
            delta_seconds = (now - worker.last_signal_time).seconds
            if delta_seconds > self.mqtt_conf.prune_timeout:
                self.workers.pop(w_id)
                print(f'pruning unresponsive worker {worker}')
                if worker.job:
                    worker.job.restore()

    def _assign_jobs(self):
        while True:
            worker = next((w for w in self.workers.values() if w.free), None)
            job = next((j for j in self.jobs.values() if j.assignable), None)
            if not worker:
                # print('no worker')
                return
            if not job:
                # print('no job')
                return
            job.assign(worker)
            worker.worker_rpc.assign_job(job.job_id, job.job_payload)
