__author__ = 'nolka'

import requests
import random

from lib.worker import BaseWorker
from lib import SimpleTask


class UrlParser(BaseWorker):
    def run(self, task):
        """
        :type url: SimpleTask
        """

        result = requests.get('%s' % task.data)

        if result.status_code == 200:
            return len(result.text)

class NamedUrlParser(UrlParser):
    def __init__(self, name):
        self.name = name