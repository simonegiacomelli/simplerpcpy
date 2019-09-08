import typing
import uuid


class Job:
    def __init__(self, job):
        self.job_id = str(uuid.uuid4())
        self.job = job
        self.done = False
        self.result = None


class JobManager:
    todo: typing.Sequence[Job] = None
    done: typing.Sequence[Job] = None

    def add(self, job: Job):
        pass

    def get_done(self, timeout=0) -> Job:
        pass
