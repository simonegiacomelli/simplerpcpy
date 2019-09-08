import uuid
from typing import Dict


class MJobAbc:
    def __init__(self, job):
        self.job_id = str(uuid.uuid4())
        self.job = job
        self.done = False
        self.result = None


class ManagerAbc:
    jobs: Dict[MJobAbc] = None

    def add(self, job) -> MJobAbc:
        raise NotImplemented()

    def get_done(self, timeout=0) -> MJobAbc:
        raise NotImplemented()


class WJob:
    def __init__(self, job_id, job):
        self.job_id = job_id
        self.job = job
        self.result = None
        self.done = False


class WorkerAbc:
    def get_job(self) -> WJob:
        raise NotImplemented()

    def job_done(self, result):
        raise NotImplemented()
