import multiprocessing

__author__ = 'nolka'

import logging
import time
from multiprocessing import Queue, Process

from lib import SimpleTask

class WorkManager(object):
    def __init__(self, result_handler=None):
        self.logger = logging.getLogger("jobmanager.WorkManager")
        # console = logging.StreamHandler()
        # console.setFormatter(logging.Formatter("%(asctime)s: %(message)s"))
        # self.logger.addHandler(console)

        self.result_handler = result_handler
        self.running_workers = 0
        self.jobs = None
        self.results = Queue()
        self.results_handler = None
        self.workers = list()
        self.logger.info("WorkManager initialized")

    def create_workers(self, klass, workers_count=None):
            self.workers_count = workers_count if workers_count is not None else multiprocessing.cpu_count()
            self.logger.debug("Creating worker threads: %d" % self.workers_count)
            self.jobs = Queue(self.workers_count)
            for i in xrange(self.workers_count):
                self._instantiate_worker(klass, 'Worker-%d' % i)

    def configure_workers(self, *configurations):
        self.logger.debug("Creating workers(%d) by configuration..." % len(configurations))
        if not self.jobs:
            self.jobs = Queue(len(configurations))

        for _id, config in enumerate(configurations):
            if isinstance(config, type):
                self._instantiate_worker(config)
                continue

            klass = config[0]
            self.logger.debug("Instantiating type %s" % klass.__name__)
            if len(config) == 1:
                self._instantiate_worker(klass, "%s-%s" % (klass.__name__, _id))
            if len(config) == 2:
                self._instantiate_worker(klass, "%s-%s" % (klass.__name__, _id), args=config[1])
            if len(config) == 3:
                self._instantiate_worker(klass, "%s-%s" % (klass.__name__, _id), args=config[1], kwargs=config[2])


    def _instantiate_worker(self, klass, name=None, args=None, kwargs=None):
        if not args and not kwargs:
            instance = klass()
        elif args and kwargs:
            instance = klass(*args, **kwargs)
        elif args and not kwargs:
            instance = klass(*args)
        elif not args and kwargs:
            instance = klass(**kwargs)

        if not hasattr(instance, 'name'):
            instance.name = name if name else "<Unnamed worker %d>" % id(instance)

        t = Process(target=instance, name=instance.name, args=(self.jobs, self.results))

        self.workers.append(t)
        self.logger.debug("Created worker %s in thread %d" % (instance.name, id(t)))

    def do_work(self):
        for t in self.workers:
            t.start()
            self.running_workers += 1

        self.result_handler = Process(target=self.result_handler, args=(self, self.results))
        self.result_handler.start()

    def add_job(self, task, task_id = None):
        if task_id is None:
            if not hasattr(self, '_jobs_added'):
                self._jobs_added = 0
            else:
                self._jobs_added += 1
            task_id = self._jobs_added

        self.jobs.put(SimpleTask(task, task_id))

    def exit(self, wait_until_exit=True):
        self.logger.debug("Running workers count %d" % self.running_workers)
        for i in xrange(self.running_workers):
            self.jobs.put(None)

        if wait_until_exit:
            for _id, w in enumerate(self.workers):
                self.workers[_id].join()
                self.running_workers -= 1

        self.jobs.close()
        self.jobs.join_thread()
        self.results.put(None)
        self.result_handler.join()
        self.results.close()
        self.results.join_thread()

        if wait_until_exit:
            _alive_threads = True
            while _alive_threads:
                for w in self.workers:
                    if w.is_alive():
                        _alive_threads = True
                    else:
                        _alive_threads = False
                if _alive_threads or self.result_handler.is_alive():
                    self.logger.warn("waiting...")
                    time.sleep(.1)
                else:
                    break
