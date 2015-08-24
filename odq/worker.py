""" Command Line Helpers """
import sys
import logging
import argparse

from pickle import loads

sys.path.insert(0, '.')

logger = logging.getLogger('odq')


def get_parser():
    parser = argparse.ArgumentParser(description='ODQ Worker')
    parser.add_argument('odq', type=str,
                        help='odq object, e.g. app:o')
    parser.add_argument(
        '--worker', '-w1', type=str, default='thread',
        choices=['gevent', 'thread', 'process'],
        help='worker type to use, DEFAULT to "thread"')
    parser.add_argument(
        '--subworker', '-w2', type=str, default='',
        choices=['gevent', 'thread', ''],
        help='additional sub worker inside worker, this is '
        'to overcome python\'s single core limit, '
        'if empty, no sub worker will be used'
        ', DEFAULT to ""')
    parser.add_argument(
        '--concurrency', '-c1', type=int, default=1,
        help='concurrency level of selected worker')
    parser.add_argument(
        '--subconcurrency', '-c2', type=int, default=1,
        help='concurrency level of selected sub worker')
    return parser


def main():
    formatter = logging.Formatter(
        '[%(asctime)s] %(name)s<%(levelname)s> %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    args = get_parser().parse_args()

    if args.subworker and args.worker != 'process':
        logger.error('using subworkers requires worker set to be "process"')
        return

    if args.worker == 'thread':
        from concurrent.futures import ThreadPoolExecutor
        e = ThreadPoolExecutor(args.concurrency)
        for _ in range(args.concurrency):
            e.submit(run_worker, args.odq, args.worker)

    elif args.worker == 'process':
        from concurrent.futures import ProcessPoolExecutor
        e = ProcessPoolExecutor(args.concurrency)
        for _ in range(args.concurrency):
            e.submit(run_worker, args.odq, args.worker,
                     args.subworker, args.subconcurrency)

    elif args.worker == 'gevent':
        from gevent import monkey
        monkey.patch_all()
        from gevent.pool import Pool
        pool = Pool(args.concurrency)
        for _ in range(args.concurrency):
            pool.spawn(run_worker, args.odq, args.worker)
        pool.join()


def run_worker(odq, worker='thread',
               subworker='', subconcurrency=1, logger=logger):

    def do_work(logger=logger):
        # TODO: support batch job execution in single worker
        # this can reduce broker overhead when there's tons of tiny jobs
        import time
        import importlib
        path, name = odq.split(':')
        m = importlib.import_module(path)
        o = getattr(m, name)

        queues = []
        for queue in o.queues:
            configs = o.configs[queue]
            max_workers = configs.get('max_workers')
            if max_workers:
                if o.num_processing(queue) < max_workers:
                    queues.append(queue)
            else:
                queues.append(queue)

        while True:
            results = o.disque_client.get_job(queues)
            for queue, jobid, payload in results:
                funcname, args, kwargs = loads(payload)
                func = getattr(m, funcname)
                try:
                    t0 = time.time()
                    result = func.__func__(*args, **kwargs)
                    seconds = time.time() - t0
                except:
                    logger.exception('executing {}(*{}, **{}) failed'
                                     ''.format(funcname, args, kwargs))
                    # TODO: log error message to error queue
                else:
                    logger.info('job {}(*{}, **{}) executed in {:.6f} '
                                'seconds, returns {}'
                                ''.format(funcname, args,
                                          kwargs, seconds, result))
                    o.disque_client.ack_job(jobid)

    if subworker == 'thread':
        import threading
        tasks = [threading.Thread(target=do_work)
                 for _ in range(subconcurrency)]
        for t in tasks:
            t.start()
        for t in tasks:
            t.join()

    elif subworker == 'gevent':
        from gevent import monkey
        from gevent.pool import Pool
        monkey.patch_all()
        pool = Pool(subconcurrency)
        for _ in range(subconcurrency):
            pool.spawn(do_work)
        pool.join()
    else:
        do_work()
