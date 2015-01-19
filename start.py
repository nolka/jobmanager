# -*- coding: utf8

__author__ = 'nolka'

import logging

from timeit import default_timer as timer

from lib import WorkManager, SimpleTask
from lib.worker.urlparser import UrlParser


def main():
    def result_handler(manager, results):
        """
        Handle worker results
        :param manager: WorkManager
        :param results: Queue
        """
        while True:
            task = results.get()
            ":type task: SimpleTask"
            if task is None:
                logging.warn("Received poison...")
                break
            logging.info("Received task id: %d with result length: %s" % (task.index, task.result))

    start = timer()
    m = WorkManager(result_handler)
    m.create_workers(UrlParser)
    m.do_work()

    listen_files = [
        "http://stackoverflow.com",
        "http://caldina-club.com",
        "http://www.land-cruiser.ru",
        "http://habrahabr.ru",
        "http://pikabu.ru",
        "http://stackoverflow.com",
        "http://caldina-club.com",
        "http://www.land-cruiser.ru",
        "http://habrahabr.ru",
        "http://pikabu.ru",
    ]

    for url in listen_files:
        logging.info("added job %s" % url)
        m.add_job(url)

    logging.warn("Finalizing :D")

    m.exit()

    logging.warn("Done!")
    print (timer()-start)


if __name__ == "__main__":
    main()