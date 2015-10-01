__author__ = 'nolka'

import logging
import traceback

from task import BaseTask


class BaseWorker(object):
    def __init__(self, name=None, manager=None):
        self.name = name
        self.manager = manager
        self.shutdown_pending = False
        self._thread_handle = None

    def listen_tasks(self, jobs, results):
        while not self.shutdown_pending:
            try:
                task = jobs.get()
                if task is None:
                    logging.warn("%s is exiting..." % self.name)
                    return

                try:
                    r = self.run(task)
                    if issubclass(r.__class__, BaseTask):
                        task = r
                    else:
                        task.done(r)

                except Exception as e:
                    logging.error("Error occurred in thread %s: %s\n%s" % (self.name, e, traceback.format_exc()))
                    task.done(None, "\n".join((e, traceback.format_exc())))
                finally:
                    results.put(task)
            except KeyboardInterrupt as e:
                logging.warn("Terminating worker by Ctrl+C")
                return None

    def run(self, task):
        raise NotImplementedError("Not implemented!")

    def start(self):
        self._thread_handle.start()

    def stop(self):
        self.shutdown_pending = True
        self._thread_handle.join()

    def __call__(self, jobs, results, *args, **kwargs):
        return self.listen_tasks(jobs, results)