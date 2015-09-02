""" Task Manager """
import time
import logging
from pickle import dumps

from crontab import CronTab

from .client import Client


logger = logging.getLogger('odq')


class Odq(object):
    queues = set()
    configs = {}

    def __init__(self, disque_client=None, queue=None,
                 debug=False, ttl=86400, retry=8640,
                 max_workers=None):
        if not disque_client:
            disque_client = Client()
        self.disque_client = disque_client
        self.debug = debug
        self.queue = queue
        self.ttl = ttl
        self.retry = retry
        self.max_workers = max_workers

    def get_config(self):
        return {
            'queue': self.queue,
            'ttl': self.ttl,
            'retry': self.retry,
            'debug': self.debug,
            'max_workers': self.max_workers,
        }

    def add_queue(self, queue):
        if self.queue is None:
            self.queues.add(queue)

    def task(self, func=None, **config):
        def wrapper(func):
            config = self.get_config()
            return wrapper_with_config(config)(func)

        def wrapper_with_config(config):
            def outer(func):
                def with_config(**config):
                    def deco(*args, **kwargs):
                        oldconfig = func.__odq__
                        newconfig = oldconfig.copy()
                        newconfig.update(config)
                        setattr(func, '__odq__', newconfig)
                        result = inner(*args, **kwargs)
                        setattr(func, '__odq__', oldconfig)
                        return result
                    return deco

                def run(*args, **kwargs):
                    return func(*args, **kwargs)

                def inner(*args, **kwargs):
                    config = func.__odq__
                    if config['debug']:
                        return func(*args, **kwargs)
                    else:
                        queue = config['queue']
                        if queue is None:
                            queue = func.__name__
                        delay = config.get('delay') or 0
                        if 'at' in config:
                            delay += config['at'].timestamp() - time.time()
                        elif 'cron' in config:
                            cron = config['cron']
                            delay += CronTab(cron).next()
                        jobid = self.disque_client.add_job(
                            queue_name=queue,
                            job=dumps([func.__name__, args, kwargs]),
                            timeout=config.get('timeout', 200),
                            replicate=config.get('replicate'),
                            delay=delay,
                            ttl=config.get('ttl'),
                            retry=config.get('retry'),
                            maxlen=config.get('maxlen'),
                            async=config.get('async', False))
                        return jobid

                    return inner

                self.add_queue(func.__name__)
                self.configs[func.__name__] = config
                setattr(func, '__odq__', config)
                setattr(inner, 'with_config', with_config)
                setattr(inner, 'run', run)
                setattr(inner, '__func__', func)
                return inner
            return outer

        if not config and callable(func):
            return wrapper(func)
        else:
            newconfig = self.get_config()
            newconfig.update(config)
            return wrapper_with_config(newconfig)

    def filter_jobs(self, queue, state, count=100):
        command = ['JSCAN', 'COUNT', count, 'QUEUE', queue, 'STATE', state]
        code, jobs = self.disque_client.execute_command(*command)
        while code != b'0':
            command = command[:1] + [code] + command[-6:]
            code, njobs = self.disque_client.execute_command(*command)
            jobs.extend(njobs)
        return jobs

    def num_processing(self, queue):
        # TODO: this is a terrible implementation
        # should rewrite when QSTAT command is available
        return len(self.filter_jobs(queue, 'active'))
