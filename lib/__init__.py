__author__ = 'nolka'

import time
from multiprocessing import Queue, Process

from tasks import SimpleTask, BaseTask


class WorkManager(object):
    def __init__(self, result_handler=None, logger=None):
        self.result_handler = result_handler
        self.logger = logger
        self.running_workers = 0
        self.jobs_added = 0
        self.jobs = None
        self.results = Queue()
        self.results_handler = None
        self.workers = list()
        self.logger.info("WorkManager initialized")

    def add_workers(self, *workers):
        if not self.jobs:
            self.jobs = Queue(len(workers))

        for _id, config in enumerate(workers):
            self._configure(config)


    def _configure(self, instance, name=None):

        if not hasattr(instance, 'name'):
            instance.name = name if name else "<Unnamed worker %d>" % id(instance)

        instance._thread_handle = Process(target=instance, name=instance.name, args=(self.jobs, self.results))

        self.workers.append(instance)
        self.logger.debug("Created worker %s in thread %d" % (instance.name, id(instance._thread_handle)))

    def do_work(self):
        for w in self.workers:
            w.start()
            self.running_workers += 1

        self.result_handler = Process(target=self.result_handler, args=(self, self.results))
        self.result_handler.start()

    def add_job(self, task, task_id=None, task_wrapper=SimpleTask):
        if task_id is None:
            task_id = self.jobs_added
            self.jobs_added += 1

        if isinstance(task, BaseTask):
            task.set_id(task_id)
            self.jobs.put(task)
        else:
            self.jobs.put(task_wrapper(task_id, data=task))

    def exit(self, wait_until_exit=True):
        self.logger.debug("Running workers count %d" % self.running_workers)
        for i in xrange(self.running_workers):
            self.jobs.put(None)

        self.logger.debug("Joining jobs thread...")
        self.jobs.close()
        self.jobs.join_thread()

        if wait_until_exit:
            for _id, w in enumerate(self.workers):
                self.logger.debug("Joining thread %s..." % _id)
                self.workers[_id].stop()
                # self.workers[_id]._thread_handle.join()
                self.running_workers -= 1

        self.logger.debug("Joining results thread...")
        self.results.put(None)
        self.result_handler.join()
        self.results.close()
        self.results.join_thread()

        if wait_until_exit:
            self.logger.debug("Waiting worker threads for exist...")
            _alive_threads = True
            while _alive_threads:
                for w in self.workers:
                    if w._thread_handle.is_alive():
                        _alive_threads = True
                    else:
                        _alive_threads = False
                if _alive_threads or self.result_handler.is_alive():
                    self.logger.warn("waiting...")
                    time.sleep(.1)
                else:
                    break
