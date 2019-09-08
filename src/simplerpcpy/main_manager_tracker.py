import time

from simplerpcpy.distributed_conf_instances import distrib_conf_test
from simplerpcpy.gmqtt_client import GmqttClient
from simplerpcpy.job_manager import Manager
from simplerpcpy.window_rate import WindowRate


def main():
    client = GmqttClient(distrib_conf_test.broker_config).connect()
    manager = Manager(distrib_conf_test, client)

    jobs_to_add = 1000
    jobs_added = 0
    counter = 0
    wr = WindowRate(10)
    left_todo = set()
    done_count = 0
    while True:
        print('rate', wr.rate(), 'nr of workers', len(manager.workers)
              , f' todo={len(left_todo)} done={done_count}',
              f'free workers={len([1 for id, w in manager.workers.items() if w.free])}')
        if counter % 10 == 0 and jobs_added < jobs_to_add:
            for b in range(1, 101):
                job = manager.add(f'{counter}+{b}')
                left_todo.add(job)
                jobs_added += 1
        counter += 1

        while True:
            job = manager.get_done()
            if not job:
                break
            wr.spin()
            done_count += 1
            left_todo -= {job}
            assert eval(job.job_payload), job.result
            # print('job done!', job.job, '=', job.result)
        time.sleep(1)


if __name__ == '__main__':
    main()
