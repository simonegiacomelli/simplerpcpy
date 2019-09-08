class ManagerRpc:

    def signal_status(self, worker_endpoint_id, call_sign, job_id, job_done, job_result):
        raise NotImplemented()


class WorkerRpc:

    def assign_job(self, job_id, job):
        raise NotImplemented()
