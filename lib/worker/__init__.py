__author__ = 'nolka'

import logging
import traceback

class BaseWorker(object):

    def listen_tasks(self, jobs, results):
        while True:
            task = jobs.get()
            if task is None:
                logging.warn("%s is exiting..." % self.name)
                break

            try:
                task.result = self.run(task)
            except Exception as e:
                logging.error("Error occurred in thread %s: %s\n%s"% (self.name, e, traceback.format_exc()))
                task.result = None
            finally:
                results.put(task)

    def run(self, task):
        raise NotImplementedError("Not implemented!")

    def __call__(self, jobs, results, *args, **kwargs):
        return self.listen_tasks(jobs, results)