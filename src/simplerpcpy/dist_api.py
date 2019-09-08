class ManagerRpc:

    def signal_presence(self, worker_endpoint_id, manager_endpoint_id, call_sign, timestamp):
        raise NotImplemented()


class ManagerFromWorkerRpc:

    def job_accepted(self, job_id):
        raise NotImplemented()

    def job_rejected(self, job_id):
        pass

    def job_done(self, job_id, result):
        raise NotImplemented()


class WorkerRpc:

    def assign_job(self, job_id, job):
        raise NotImplemented()
