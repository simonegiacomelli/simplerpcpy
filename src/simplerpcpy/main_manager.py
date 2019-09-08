import time

from simplerpcpy.distributed_conf import distrib_conf_test
from simplerpcpy.gmqtt_client import GmqttClient
from simplerpcpy.job_manager import Job
from simplerpcpy.rpc_manager import Manager
from simplerpcpy.window_rate import WindowRate


def main():
    client = GmqttClient(distrib_conf_test.broker_config).connect()
    manager = Manager(distrib_conf_test, client)

    counter = 0
    wr = WindowRate(10)
    left_todo = set()
    done_count = 0
    while True:
        print('rate', wr.rate(), 'nr of workers', len(manager.workers)
              , f' todo={len(left_todo)} done={done_count}',
              f'free workers={len([1 for id, w in manager.workers.items() if w.free])}')
        if counter % 10 == 0:
            for b in range(1, 21):
                job = Job(f'{counter}+{b}')
                manager.add(job)
                left_todo.add(job)
        counter += 1

        timeout = 1
        while True:
            job = manager.get_done()
            timeout = 0
            if not job:
                break
            wr.spin()
            done_count += 1
            left_todo -= {job}
            assert eval(job.job), job.result
            # print('job done!', job.job, '=', job.result)
        time.sleep(1)


if __name__ == '__main__':
    main()
