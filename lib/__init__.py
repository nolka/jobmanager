import multiprocessing

__author__ = 'nolka'

import logging
import time
from multiprocessing import Queue, Process

class SimpleTask(object):
    __slots__ = ['index', 'data', 'result']
    def __init__(self, data, index=None):
        self.index = index
        self.data = data

    def __unicode__(self):
        return '%s' % self.data

    def __str__(self):
        return unicode(self.data).encode('utf-8')

class WorkManager(object):
    def __init__(self, result_handler=None):
        logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.DEBUG)
        self.result_handler = result_handler
        self.running_workers = 0
        self.jobs = None
        self.results = Queue()
        self.results_handler = None
        self.workers = list()
        logging.info("WorkManager initialized")

    def create_workers(self, klass, workers_count=None):
        self.workers_count = workers_count if workers_count else multiprocessing.cpu_count()
        logging.debug("Creating worker threads: %d" % self.workers_count)
        self.jobs = Queue(workers_count)
        for i in range(self.workers_count):
            instance = klass()
            t = Process(target=instance, args=(self.jobs, self.results))
            instance.name = "Worker-%d" % i
            logging.info("Created worker %s" % instance.name)
            self.workers.append(t)

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
        logging.info("Running workers count %d" % self.running_workers)
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
                    print "waiting..."
                    time.sleep(.1)
                else:
                    break
